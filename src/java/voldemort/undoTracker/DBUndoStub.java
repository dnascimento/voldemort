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
     * @paramrud
     * @param reqBid: Request Branch ID (separate redo attempts)
     */
    public void getStart(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(rud.branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.getStart(key.clone(), rud, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.getStart(key.clone(), rud, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.getStart(key.clone(), rud, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.getStart(key.clone(), rud, sts);
                }
            }
            log.info(rud.rid + " : get key: " + hexStringToAscii(key) + " branch: " + rud.branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, rud.branch, snapshotVersion);
        } else {
            log.info(rud.rid + " : get key: " + hexStringToAscii(key) + " branch: " + rud.branch);
        }
    }

    public void putStart(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(rud.branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.putStart(key.clone(), rud, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.putStart(key.clone(), rud, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.putStart(key.clone(), rud, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.putStart(key.clone(), rud, sts);

                }
            }
            log.info(rud.rid + " : put key: " + hexStringToAscii(key) + " branch: " + rud.branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, rud.branch, snapshotVersion);
        } else {
            log.info(rud.rid + " : put key: " + hexStringToAscii(key) + " branch: " + rud.branch);
        }
    }

    public void deleteStart(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(rud.branch <= bid) {
                // new request for old branch - read old branch (req. BranchId)
                snapshotVersion = newRequestsScheduler.deleteStart(key.clone(), rud, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    // new request but may need to wait to avoid dirty reads
                    snapshotVersion = restrainScheduler.deleteStart(key.clone(), rud, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.deleteStart(key.clone(), rud, sts);
                } else {
                    // redo requests: any change is performed in newest branch
                    snapshotVersion = redoScheduler.deleteStart(key.clone(), rud, sts);
                }
            }
            log.info(rud.rid + " : delete key: " + hexStringToAscii(key) + " branch: " + rud.branch
                     + " snapshot: " + snapshotVersion);
            modifyKey(key, rud.branch, snapshotVersion);
        } else {
            log.info(rud.rid + " : delete key: " + hexStringToAscii(key) + " branch: " + rud.branch);
        }
    }

    public void getEnd(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(rud.branch <= bid) {
                newRequestsScheduler.getEnd(key.clone(), rud);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    restrainScheduler.getEnd(key.clone(), rud);
                    newRequestsScheduler.getEnd(key.clone(), rud);
                } else {
                    redoScheduler.getEnd(key.clone(), rud);
                }
            }
        }
    }

    public void putEnd(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(rud.branch <= bid) {
                newRequestsScheduler.putEnd(key.clone(), rud);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    restrainScheduler.putEnd(key.clone(), rud);
                    newRequestsScheduler.putEnd(key.clone(), rud);
                } else {
                    redoScheduler.putEnd(key.clone(), rud);
                }
            }
        }
    }

    public void deleteEnd(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            removeKeyVersion(key);
            short bid = (short) bidAtomic.get();
            if(rud.branch <= bid) {
                newRequestsScheduler.deleteEnd(key.clone(), rud);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    restrainScheduler.deleteEnd(key.clone(), rud);
                    newRequestsScheduler.deleteEnd(key.clone(), rud);
                } else {
                    redoScheduler.deleteEnd(key.clone(), rud);
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
        System.out.println("restrain phase is over");
        // execute all pendent requests
        synchronized(restrainAtomic) {
            restrainAtomic.notifyAll();
        }
    }

    /**
     * Invoked before PUT to check the proper version.
     * 
     * @param key
     * @param rud
     */
    public void getVersion(ByteArray key, RUD rud) {
        if(rud.rid != 0) {
            long sts = stsAtomic.get(); // season
            short bid = (short) bidAtomic.get();
            long snapshotVersion;
            if(rud.branch <= bid) {
                snapshotVersion = newRequestsScheduler.getVersionStart(key.clone(), rud, sts);
            } else {
                boolean isContaining = restrainAtomic.get();
                if(rud.restrain) {
                    assert (isContaining == true);
                    snapshotVersion = restrainScheduler.getVersionStart(key.clone(), rud, sts);
                    bid = (short) bidAtomic.get();
                    snapshotVersion = newRequestsScheduler.getVersionStart(key.clone(), rud, sts);
                } else {
                    snapshotVersion = redoScheduler.getVersionStart(key.clone(), rud, sts);

                }
            }
            log.info(rud.rid + " : getVersion key: " + hexStringToAscii(key) + " branch: "
                     + rud.branch + " snapshot: " + snapshotVersion);
            modifyKey(key, rud.branch, snapshotVersion);
        } else {
            log.info(rud.rid + " : getVersion key: " + hexStringToAscii(key) + " branch: "
                     + rud.branch);
        }
    }
}
