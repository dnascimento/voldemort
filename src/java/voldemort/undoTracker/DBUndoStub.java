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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
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

    private final static Logger log = LogManager.getLogger(DBUndoStub.class.getName());
    Object restrainLocker = new Object();
    BranchController brancher = new BranchController();

    RedoScheduler redoScheduler;
    CommitScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

    public DBUndoStub() {
        this(loadKeyAccessList());
    }

    /**
     * One instance for multiple handlers
     * 
     * @throws IOException
     */
    public DBUndoStub(OpMultimap keyAccessLists) {
        DOMConfigurator.configure("log4j.xml");
        Runtime.getRuntime().addShutdownHook(new SaveKeyAccess(keyAccessLists));

        System.out.println("New DBUndo stub");
        this.keyAccessLists = keyAccessLists;

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new CommitScheduler(keyAccessLists);
        restrainScheduler = new RestrainScheduler(keyAccessLists, restrainLocker);
        new InvertDependencies(keyAccessLists).start();
        try {
            new ServiceDBNode(this).start();
        } catch(IOException e) {
            log.error("DBUndoStub", e);
        }
    }

    /**
     * 
     * @param key
     * @param rud
     * @param reqBid: Request Branch ID (separate redo attempts)
     */
    public void getStart(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            StsBranchPair current = brancher.getCurrent();
            StsBranchPair access;
            if(rud.branch <= current.branch) {
                // Read old/common branch, may create a new commit
                access = newRequestsScheduler.getStart(key.clone(), rud, current);
            } else {
                // use redo branch
                if(rud.restrain) {
                    // new request but may need to wait to avoid dirty reads
                    restrainScheduler.getStart(key.clone(), rud, current);
                    current = brancher.getCurrent();
                    rud.branch = current.branch; // update the requests branch
                    access = newRequestsScheduler.getStart(key.clone(), rud, current);
                } else {
                    // redo requests
                    StsBranchPair redoBase = new StsBranchPair(rud.getBaseSts(), -1);
                    access = redoScheduler.getStart(key.clone(), rud, redoBase);
                }
            }
            log.info(rud.rid + " : get key: " + hexStringToAscii(key) + " branch: " + access.branch
                     + " commit: " + access.sts);
            modifyKey(key, access.branch, access.sts);
        } else {
            log.info(rud.rid + " : get key: " + hexStringToAscii(key) + " branch: " + rud.branch);
        }
    }

    public void getEnd(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(rud.branch <= bid) {
                newRequestsScheduler.getEnd(key.clone(), rud);
            } else {
                if(rud.restrain) {
                    restrainScheduler.getEnd(key.clone(), rud);
                    newRequestsScheduler.getEnd(key.clone(), rud);
                } else {
                    redoScheduler.getEnd(key.clone(), rud);
                }
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
        if(rud.branch <= rud.branch) {
            log.error("Unlocking in the wrong branch");
            throw new VoldemortException("Unlocking in the wrong branch");
        } else {
            for(ByteArray key: keys) {
                redoScheduler.unlock(key.clone(), rud);
            }
        }
        return result;
    }

    /**
     * Invoked by manager to start a new commit, in the current branch
     * 
     * @param newRid
     */
    public void setNewCommitRid(long newRid) {
        SimpleDateFormat formater = new SimpleDateFormat("hh:mm:ss dd/mm/yyyy");
        String time = formater.format(new Date(newRid));
        log.info("Commit scheduled with rid: " + newRid + " " + time);
        brancher.setCommit(newRid);
    }

    public void unlockRestrain(short branch, long sts) {
        // done, increase the bid, the redo is over: new branch and commit
        brancher.setBranchAndCommit(branch, sts);
        System.out.println("restrain phase is over, new branch is:" + branch + " based on commit: "
                           + sts);
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
        byte[] version = ByteBuffer.allocate(key.length() + 10)
                                   .put(key.get(), 0, key.length())
                                   .putShort(branch)
                                   .putLong(commit)
                                   .array();
        key.set(version);
        return key;
    }

    public static long getKeyCommit(ByteArray key) {
        byte[] kb = key.get();
        if(kb.length < 8) {
            return -1;
        }
        byte[] longBytes = Arrays.copyOfRange(kb, kb.length - 8, kb.length);
        ByteBuffer bb = ByteBuffer.wrap(longBytes);
        return bb.getLong();
    }

    public static void removeKeyVersion(ByteArray key) {
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
     * @return
     */
    private static OpMultimap loadKeyAccessList() {
        OpMultimap list;
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
        return new OpMultimap();
    }
}
