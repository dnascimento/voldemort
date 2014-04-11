package voldemort.undoTracker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.schedulers.RedoScheduler;
import voldemort.undoTracker.schedulers.RestrainScheduler;
import voldemort.undoTracker.schedulers.SnapshotScheduler;
import voldemort.utils.ByteArray;

/**
 * Singleton class used by multiple threads
 * 
 * @author darionascimento
 * 
 */
public class DBUndoStub {

    private final Logger log = LogManager.getLogger("LockArray");
    /**
     * Snapshot Timestamp or Snapshot RID: defines the next snapshot if >
     * current moment or the last snapshot otherwise
     */
    AtomicLong stsAtomic = new AtomicLong(1);
    AtomicInteger bidAtomic = new AtomicInteger(1);
    AtomicBoolean restrainAtomic = new AtomicBoolean(false);

    RedoScheduler redoScheduler;
    SnapshotScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

    public DBUndoStub() {
        this(new OpMultimap());
    }

    /**
     * One instance for multiple protobuffrequesthandler
     * 
     * @throws IOException
     */
    public DBUndoStub(OpMultimap keyAccessLists) {
        DOMConfigurator.configure("log4j.xml");
        System.out.println("New DBUndo stub");
        this.keyAccessLists = keyAccessLists;

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new SnapshotScheduler(keyAccessLists);
        restrainScheduler = new RestrainScheduler(keyAccessLists, restrainAtomic);
        new InvertDependencies(keyAccessLists).start();
        try {
            new ManagerCommands(this).start();
        } catch(IOException e) {
            log.error("DBUndoStub", e);
        }
    }

    /**
     * 
     * @param key
     * @param rid
     * @param reqBid: Request Branch ID (separate redo attempts)
     * @param contained
     */
    public void getStart(ByteArray key, long rid, short branch, boolean contained) {
        if(rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.getStart(key.clone(), rid, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.getStart(key.clone(), rid, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.getStart(key.clone(), rid, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.getStart(key.clone(), rid, sts);
                }
            }
            log.info(rid + " : get key: " + hexStringToAscii(key) + " branch: " + branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, branch, snapshotVersion);
        }
    }

    public void putStart(ByteArray key, long rid, short branch, boolean contained) {
        if(rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.putStart(key.clone(), rid, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.putStart(key.clone(), rid, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.putStart(key.clone(), rid, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.putStart(key.clone(), rid, sts);

                }
            }
            log.info(rid + " : put key: " + hexStringToAscii(key) + " branch: " + branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, branch, snapshotVersion);
        }
    }

    public void deleteStart(ByteArray key, long rid, short branch, boolean contained) {
        if(rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.deleteStart(key.clone(), rid, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.deleteStart(key.clone(), rid, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.deleteStart(key.clone(), rid, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.deleteStart(key.clone(), rid, sts);
                }
            }
            log.info(rid + " : delete key: " + hexStringToAscii(key) + " branch: " + branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, branch, snapshotVersion);
        }
    }

    public void getEnd(ByteArray key, long rid, short reqBid, boolean contained) {
        if(rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(reqBid <= bid) {
                newRequestsScheduler.getEnd(key.clone(), rid);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    restrainScheduler.getEnd(key.clone(), rid);
                    newRequestsScheduler.getEnd(key.clone(), rid);
                } else {
                    redoScheduler.getEnd(key.clone(), rid);
                }
            }
        }
    }

    public void putEnd(ByteArray key, long rid, short reqBid, boolean contained) {
        if(rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(reqBid <= bid) {
                newRequestsScheduler.putEnd(key.clone(), rid);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    restrainScheduler.putEnd(key.clone(), rid);
                    newRequestsScheduler.putEnd(key.clone(), rid);
                } else {
                    redoScheduler.putEnd(key.clone(), rid);
                }
            }
        }
    }

    public void deleteEnd(ByteArray key, long rid, short reqBid, boolean contained) {
        if(rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(reqBid <= bid) {
                newRequestsScheduler.deleteEnd(key.clone(), rid);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(contained) {
                    assert (isContaining == true);
                    restrainScheduler.deleteEnd(key.clone(), rid);
                    newRequestsScheduler.deleteEnd(key.clone(), rid);
                } else {
                    redoScheduler.deleteEnd(key.clone(), rid);
                }
            }

        }
    }

    public static String hexStringToAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {}
        return key.toString();
    }

    public void setNewSnapshotRid(long newRid) {
        log.info("New SnapshotRid: " + newRid);
        stsAtomic.set(newRid);
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
    public static ByteArray modifyKey(ByteArray key, short branch, long snapshot) {
        byte[] version = ByteBuffer.allocate(key.length() + 10)
                                   .put(key.get(), 0, key.length())
                                   .putShort(branch)
                                   .putLong(snapshot)
                                   .array();
        key.set(version);
        return key;
    }

    public static long getKeySnapshot(ByteArray key) {
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

    public void restrainEnable() {
        restrainAtomic.set(true);

    }

    public void restrainDisable() {
        restrainAtomic.set(false);
        // done, increase the bid, the redo is over
        bidAtomic.incrementAndGet();
        System.out.println("Retrain phase is over");
        // execute all pendent requests
        synchronized(restrainAtomic) {
            restrainAtomic.notifyAll();
        }
    }
}
