package voldemort.undoTracker;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.schedulers.AccessSchedule;
import voldemort.undoTracker.schedulers.CurrentSnapshotScheduler;
import voldemort.undoTracker.schedulers.PastSnapshotScheduler;
import voldemort.utils.ByteArray;

public class DBUndoStub implements AccessSchedule {

    private final Logger log = LogManager.getLogger("LockArray");
    AtomicLong snapshotRid = new AtomicLong(0);
    PastSnapshotScheduler past;
    CurrentSnapshotScheduler current;

    /**
     * Each request handler has a UndoStub instance
     */
    public DBUndoStub() {
        this(new OpMultimap());
    }

    /**
     * Each request handler has a UndoStub instance
     */
    public DBUndoStub(OpMultimap archive) {
        DOMConfigurator.configure("log4j.xml");

        ManagerCommands.register(this);
        past = new PastSnapshotScheduler(archive);
        current = new CurrentSnapshotScheduler(archive);
    }

    @Override
    public void getStart(ByteArray key, long rid) {
        if(rid != 0) {
            if(rid < snapshotRid.get()) {
                past.getStart(key, rid);
            } else {
                current.getStart(key, rid);
            }
        }
        System.out.println(rid + " : get key: " + hexStringToAscii(key));
    }

    @Override
    public void putStart(ByteArray key, long rid) {
        if(rid < snapshotRid.get()) {
            past.putStart(key, rid);
        } else {
            current.putStart(key, rid);
        }
        System.out.println(rid + " : put key: " + hexStringToAscii(key));
    }

    @Override
    public void deleteStart(ByteArray key, long rid) {
        if(rid < snapshotRid.get()) {
            past.deleteStart(key, rid);
        } else {
            current.deleteStart(key, rid);
        }
        System.out.println(rid + " : delete key: " + hexStringToAscii(key));
    }

    @Override
    public void getEnd(ByteArray key, long rid) {
        if(rid < snapshotRid.get()) {
            past.getEnd(key, rid);
        } else {
            current.getEnd(key, rid);
        }
    }

    @Override
    public void putEnd(ByteArray key, long rid) {
        if(rid < snapshotRid.get()) {
            past.putEnd(key, rid);
        } else {
            current.putEnd(key, rid);
        }

    }

    @Override
    public void deleteEnd(ByteArray key, long rid) {
        if(rid < snapshotRid.get()) {
            past.deleteEnd(key, rid);
        } else {
            current.deleteEnd(key, rid);
        }
    }

    public static String hexStringToAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {}
        return key.toString();
    }

    public void setNewSnapshotRid(long newRid) {
        System.out.println("New SnapshotRid: " + newRid);
        snapshotRid.set(newRid);
    }

}
