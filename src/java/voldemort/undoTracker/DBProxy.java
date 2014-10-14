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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.branching.Path;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.undoTracker.schedulers.CommitScheduler;
import voldemort.undoTracker.schedulers.RedoScheduler;
import voldemort.undoTracker.schedulers.RestrainScheduler;
import voldemort.utils.ByteArray;

import com.google.protobuf.ByteString;

/**
 * Singleton class used by multiple threads
 * 
 * @author darionascimento
 * 
 */
public class DBProxy {

    public static final int MY_PORT = 11200;
    public static final InetSocketAddress MANAGER_ADDRESS = new InetSocketAddress("manager", 11000);
    static final String KEY_ACCESS_LIST_FILE = "keyaccessList.obj";

    private static final boolean LOAD_FROM_FILE = false;

    private static final Logger log = Logger.getLogger(DBProxy.class.getName());
    Object restrainLocker = new Object();
    BranchController brancher = new BranchController();

    RedoScheduler redoScheduler;
    CommitScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

    private boolean debugging;

    public DBProxy() {
        this(loadKeyAccessList(LOAD_FROM_FILE));
    }

    /**
     * One instance for multiple handlers
     * 
     * @throws IOException
     */
    private DBProxy(OpMultimap keyAccessLists) {
        DOMConfigurator.configure("log4j.xml");
        LogManager.getRootLogger().setLevel(Level.DEBUG);
        debugging = log.isInfoEnabled();

        Runtime.getRuntime().addShutdownHook(new SaveKeyAccess(keyAccessLists));

        log.info("New DBUndo stub");
        this.keyAccessLists = keyAccessLists;

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new CommitScheduler(keyAccessLists);
        restrainScheduler = new RestrainScheduler(keyAccessLists, restrainLocker);

        new SendDependencies(keyAccessLists).start();
        try {
            new ServiceDBNode(this).start();
        } catch(IOException e) {
            log.error("DBUndoStub", e);
        }
    }

    private void opStart(OpType op, ByteArray key, SRD srd) {
        if(srd.rid == 0) {
            if(debugging) {
                log.info(op + " " + hexStringToAscii(key) + " with srd: " + srd);
            }
            return;
        }
        StringBuilder sb = null;
        if(debugging) {
            sb = new StringBuilder();
        }
        Path p = brancher.getPath(srd.branch);
        BranchPath path = p.path;
        StsBranchPair access;
        if(p.isRedo) {
            // use redo branch
            if(debugging) {
                sb.append(" -> REDO: ");
                log.info("Redo Start: " + op + " " + hexStringToAscii(key) + " " + srd);
            }
            access = redoScheduler.opStart(op, key.shadow(), srd, path);
        } else {
            if(srd.restrain) {
                if(debugging) {
                    System.out.println(" -> RESTRAIN: ");
                }
                // new request but may need to wait to avoid dirty reads
                restrainScheduler.opStart(op, key.shadow(), srd, path);
                path = brancher.getCurrent();
                srd.branch = path.current.branch; // update the branch
            }
            if(log.isInfoEnabled()) {
                sb.append(" -> DO: ");
            }
            // Read old/common branch, may create a new commit - for new
            // requests
            access = newRequestsScheduler.opStart(op, key.shadow(), srd, path);
        }
        modifyKey(key, access.branch, access.sts);
        if(debugging) {
            sb.append(srd.rid);
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

    public void opEnd(OpType op, ByteArray key, SRD srd) {
        if(srd.rid != 0) {
            Path p = brancher.getPath(srd.branch);
            BranchPath path = p.path;
            removeKeyVersion(key);
            if(p.isRedo) {
                if(srd.restrain) {
                    restrainScheduler.opEnd(op, key.shadow(), srd, path);
                    newRequestsScheduler.opEnd(op, key.shadow(), srd, path);
                } else {
                    redoScheduler.opEnd(op, key.shadow(), srd, path);
                }
            } else {
                newRequestsScheduler.opEnd(op, key.shadow(), srd, path);
            }
        }
    }

    /**
     * Unlock operation to simulate the redo of these keys operations
     * 
     * @param keys
     * @param srd
     * @return
     * @throws VoldemortException
     */
    public Map<ByteArray, Boolean> unlockKey(List<ByteArray> keys, SRD srd)
            throws VoldemortException {
        HashMap<ByteArray, Boolean> result = new HashMap<ByteArray, Boolean>();
        Path p = brancher.getPath(srd.branch);

        if(p.isRedo) {
            for(ByteArray key: keys) {
                boolean status = redoScheduler.ignore(key.shadow(), srd, p.path);
                result.put(key, status);
            }
        } else {
            log.error("Unlocking in the wrong branch");
            for(ByteArray key: keys) {
                result.put(key, false);
            }
            throw new VoldemortException("Unlocking in the wrong branch");
        }
        if(debugging) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unlocked: ");
            for(ByteArray key: keys) {
                sb.append(hexStringToAscii(key));
                sb.append(" : ");
            }
            sb.append("by request: ");
            sb.append(srd.rid);
            log.info(sb.toString());
        }
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

    /**
     * Invoked by the manager before starting the replay
     * 
     * @param redoPath
     */
    public void newRedo(BranchPath redoPath) {
        log.info("New redo with path: " + redoPath);
        brancher.newRedo(redoPath);
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
    public static ByteArray modifyKey(ByteArray key, short branch, long commit) {
        byte[] oldKey = key.get();
        byte[] newKey = new byte[oldKey.length + 10];
        int i = 0;
        for(i = 0; i < oldKey.length; i++) {
            newKey[i] = oldKey[i];
        }

        newKey[i++] = (byte) (branch >> 8);
        newKey[i++] = (byte) (branch);

        newKey[i++] = (byte) (commit >> 56);
        newKey[i++] = (byte) (commit >> 48);
        newKey[i++] = (byte) (commit >> 40);
        newKey[i++] = (byte) (commit >> 32);
        newKey[i++] = (byte) (commit >> 24);
        newKey[i++] = (byte) (commit >> 16);
        newKey[i++] = (byte) (commit >> 8);
        newKey[i++] = (byte) (commit);
        key.set(newKey);
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

        byte[] longBytes = Arrays.copyOfRange(kb, 0, kb.length - 10);
        key.set(longBytes);
    }

    public void resetDependencies() {
        log.info("Reset dependency map");
        keyAccessLists.clear();
        brancher.reset();
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

    public void getStart(ByteArray key, SRD srd) {
        opStart(OpType.Get, key, srd);
    }

    public void getEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Get, key, srd);
    }

    public void putStart(ByteArray key, SRD srd) {
        opStart(OpType.Put, key, srd);
    }

    public void putEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Put, key, srd);
    }

    public void deleteStart(ByteArray key, SRD srd) {
        opStart(OpType.Delete, key, srd);
    }

    public void deleteEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Delete, key, srd);
    }

    public void getVersion(ByteArray key, SRD srd) {
        opStart(OpType.GetVersion, key, srd);
    }

    /**
     * Get the access list of a specific key for selective replay
     * 
     * @param keysList
     * @param baseRid
     * @return
     */
    public HashMap<ByteString, ArrayList<Op>> getAccessList(List<ByteString> keysList, long baseRid) {
        return keyAccessLists.getAccessList(keysList, baseRid);
    }

}
