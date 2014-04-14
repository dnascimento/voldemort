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

    public static final int MY_PORT = 11200;
    public static final InetSocketAddress MANAGER_ADDRESS = new InetSocketAddress("localhost",
                                                                                  11000);
    static final String KEY_ACCESS_LIST_FILE = "keyaccessList.obj";

    private final static Logger log = LogManager.getLogger(DBUndoStub.class.getName());
    /**
     * Snapshot Timestamp or Snapshot RID: defines the next snapshot if >
     * current moment or the last snapshot otherwise
     */
    AtomicLong stsAtomic = new AtomicLong(1);
    AtomicInteger bidAtomic = new AtomicInteger(1);
    Object restrainLocker = new Object();

    RedoScheduler redoScheduler;
    SnapshotScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

    public DBUndoStub() {
        this(loadKeyAccessList());
    }

    /**
     * One instance for multiple protobuffrequesthandler
     * 
     * @throws IOException
     */
    public DBUndoStub(OpMultimap keyAccessLists) {
        DOMConfigurator.configure("log4j.xml");
        Runtime.getRuntime().addShutdownHook(new SaveKeyAccess(keyAccessLists));

        System.out.println("New DBUndo stub");
        this.keyAccessLists = keyAccessLists;

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new SnapshotScheduler(keyAccessLists);
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
                if(rud.restrain) {
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
                if(rud.restrain) {
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
                if(rud.restrain) {
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
                if(rud.restrain) {
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
                if(rud.restrain) {
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
                if(rud.restrain) {
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
        SimpleDateFormat formater = new SimpleDateFormat("hh:mm:ss dd/mm/yyyy");
        String time = formater.format(new Date(newRid));
        log.info("Snapshot scheduled with rid: " + newRid + " " + time);
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

    public void unlockRestrain(short branch) {
        // done, increase the bid, the redo is over
        bidAtomic.set(branch);
        System.out.println("restrain phase is over, new branch is:" + branch);
        // execute all pendent requests
        synchronized(restrainLocker) {
            restrainLocker.notifyAll();
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
                if(rud.restrain) {
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

    public void resetDependencies() {
        log.info("Reset dependency map");
        keyAccessLists.clear();
    }

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
