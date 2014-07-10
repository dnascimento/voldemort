/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.branching.Path;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.undoTracker.schedulers.CommitScheduler;
import voldemort.undoTracker.schedulers.RedoScheduler;
import voldemort.undoTracker.schedulers.RestrainScheduler;
import voldemort.utils.ByteArray;

/**
 * Singleton class used by multiple threads
 * 
 * @author darionascimento
 * 
 */
public class DBUndoStub {

    public static final int MY_PORT = 11200;
    public static final InetSocketAddress MANAGER_ADDRESS = new InetSocketAddress("localhost",
                                                                                  11000);
    static final String KEY_ACCESS_LIST_FILE = "keyaccessList.obj";

    private static final boolean LOAD_FROM_FILE = false;

    private final static Logger log = Logger.getLogger(DBUndoStub.class.getName());
    Object restrainLocker = new Object();
    BranchController brancher = new BranchController();

    RedoScheduler redoScheduler;
    CommitScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

    public DBUndoStub() {
        this(loadKeyAccessList(LOAD_FROM_FILE), false);
    }

    public DBUndoStub(boolean testing) {
        this(new OpMultimap(), testing);
    }

    /**
     * One instance for multiple handlers
     * 
     * @throws IOException
     */
    private DBUndoStub(OpMultimap keyAccessLists, boolean testing) {
        DOMConfigurator.configure("log4j.xml");
        Runtime.getRuntime().addShutdownHook(new SaveKeyAccess(keyAccessLists));

        log.info("New DBUndo stub");
        this.keyAccessLists = keyAccessLists;

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new CommitScheduler(keyAccessLists);
        restrainScheduler = new RestrainScheduler(keyAccessLists, restrainLocker);
        if(!testing) {
            new InvertDependencies(keyAccessLists).start();
            try {
                new ServiceDBNode(this).start();
            } catch(IOException e) {
                log.error("DBUndoStub", e);
            }
        }
    }

    private void opStart(OpType op, ByteArray key, RUD rud) {
        if(rud.rid == 0) {
            log.info(op + " " + hexStringToAscii(key) + " with rud: " + rud);
            return;
        }
        StringBuilder sb = new StringBuilder();
        Path p = brancher.getPath(rud.branch);
        BranchPath path = p.path;
        StsBranchPair access;
        if(p.isRedo) {
            // use redo branch
            sb.append(" -> REDO: ");
            log.info("Redo Start: " + op + " " + hexStringToAscii(key) + " " + rud);
            access = redoScheduler.opStart(op, key.clone(), rud, path);
        } else {
            if(rud.restrain) {
                sb.append(" -> RESTRAIN: ");
                // new request but may need to wait to avoid dirty reads
                restrainScheduler.opStart(op, key.clone(), rud, path);
                path = brancher.getCurrent();
                rud.branch = path.current.branch; // update the branch
                sb.append(" -> AFTER RESTRAIN: ");
            }
            sb.append(" -> DO: ");
            // Read old/common branch, may create a new commit - for new
            // requests
            access = newRequestsScheduler.opStart(op, key.clone(), rud, path);
        }
        modifyKey(key, access.branch, access.sts);
        if(log.isInfoEnabled()) {
            sb.append(rud.rid);
            sb.append(" : ");
            sb.append(op);
            sb.append(" key: ");
            sb.append(hexStringToAscii(key));
            sb.append(" branch: ");
            sb.append(access.branch);
            sb.append(" commit: ");
            sb.append(access.sts);
            log.info(sb.toString());
        }
    }

    public void opEnd(OpType op, ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            Path p = brancher.getPath(rud.branch);
            BranchPath path = p.path;
            removeKeyVersion(key);
            Boolean isRedo = brancher.isRedo(rud.branch);
            if(isRedo) {
                if(rud.restrain) {
                    restrainScheduler.opEnd(op, key.clone(), rud, path);
                    newRequestsScheduler.opEnd(op, key.clone(), rud, path);
                } else {
                    redoScheduler.opEnd(op, key.clone(), rud, path);
                }
            } else {
                newRequestsScheduler.opEnd(op, key.clone(), rud, path);
            }
        }
    }

    /**
     * Unlock operation to simulate the redo of these keys operations
     * 
     * @param keys
     * @param rud
     * @return
     * @throws VoldemortException
     */
    public Map<ByteArray, Boolean> unlockKey(List<ByteArray> keys, RUD rud)
            throws VoldemortException {
        HashMap<ByteArray, Boolean> result = new HashMap<ByteArray, Boolean>();
        Path p = brancher.getPath(rud.branch);

        Boolean isRedo = brancher.isRedo(rud.branch);
        if(isRedo) {
            for(ByteArray key: keys) {
                redoScheduler.ignore(key.clone(), rud, p.path);
            }
        } else {
            log.error("Unlocking in the wrong branch");
            throw new VoldemortException("Unlocking in the wrong branch");
        }
        log.info("Unlocked: " + keys + " by " + rud);
        return result;
    }

    /**
     * Invoked by manager to start a new commit, in the current branch
     * 
     * @param newRid
     */
    public void scheduleNewCommit(long newRid) {
        String dateString = new SimpleDateFormat("H:m:S").format(new Date(newRid));
        log.info("Commit scheduled with rid: " + newRid + " at " + dateString);
        brancher.newCommit(newRid);
    }

    public void newRedo(BranchPath redoPath) {
        log.info("New redo with path: " + redoPath);
        brancher.newRedo(redoPath);
        keyAccessLists.debugExecutionList();
    }

    /**
     * Redo is over
     * 
     * @param s
     * 
     * @param branch
     * @param sts
     */
    public void redoOver() {
        brancher.redoOver();
        // execute all pendent requests
        synchronized(restrainLocker) {
            restrainLocker.notifyAll();
        }
    }

    public static String hexStringToAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {}
        return key.toString();
    }

    /**
     * Keyformat:
     * Xbytes - key
     * 2bytes - branch
     * 8bytes - sts
     * 
     * @param key
     * @param sts
     * @param branch
     */
    public ByteArray modifyKey(ByteArray key, short branch, long commit) {
        byte[] version = ByteBuffer.allocate(key.length() + 10)
                                   .put(key.get(), 0, key.length())
                                   .putShort(branch)
                                   .putLong(commit)
                                   .array();
        key.set(version);
        return key;
    }

    public long getKeyCommit(ByteArray key) {
        byte[] kb = key.get();
        if(kb.length < 8) {
            return -1;
        }
        byte[] longBytes = Arrays.copyOfRange(kb, kb.length - 8, kb.length);
        ByteBuffer bb = ByteBuffer.wrap(longBytes);
        return bb.getLong();
    }

    public void removeKeyVersion(ByteArray key) {
        byte[] kb = key.get();
        assert (kb.length > 10);

        byte[] longBytes = Arrays.copyOfRange(kb, 0, kb.length - 10);
        key.set(longBytes);
    }

    public void resetDependencies() {
        log.info("Reset dependency map");
        keyAccessLists.clear();
    }

    /**
     * Load the map from file
     * 
     * @param loadFromFile
     * 
     * @return
     */
    private static OpMultimap loadKeyAccessList(boolean loadFromFile) {
        OpMultimap list;
        if(loadFromFile) {
            try {
                FileInputStream fin = new FileInputStream(KEY_ACCESS_LIST_FILE);
                ObjectInputStream ois = new ObjectInputStream(fin);
                list = (OpMultimap) ois.readObject();
                ois.close();
                log.info("key access list file loaded");
                return list;
            } catch(Exception e) {
                log.info("No KeyAccessList file founded");
            }
        }
        return new OpMultimap();
    }

    public void getStart(ByteArray key, RUD rud) {
        opStart(OpType.Get, key, rud);
    }

    public void getEnd(ByteArray key, RUD rud) {
        opEnd(OpType.Get, key, rud);
    }

    public void putStart(ByteArray key, RUD rud) {
        opStart(OpType.Put, key, rud);
    }

    public void putEnd(ByteArray key, RUD rud) {
        opEnd(OpType.Put, key, rud);
    }

    public void deleteStart(ByteArray key, RUD rud) {
        opStart(OpType.Delete, key, rud);
    }

    public void deleteEnd(ByteArray key, RUD rud) {
        opEnd(OpType.Delete, key, rud);
    }

    public void getVersion(ByteArray key, RUD rud) {
        opStart(OpType.GetVersion, key, rud);
    }

}
