package voldemort.undoTracker.map;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

/**
 * A HashMap where same items are append to a list and each entry has a
 * read/write locker to schedule the access
 * 
 * @author darionascimento
 * 
 */
public class OpMultimap {

    private final Logger log = LogManager.getLogger("OpMultimap");
    private final int N_PARTITIONS = 16;
    /**
     * The hash table
     */
    private HashMap<ByteArray, OpMultimapEntry> map = new HashMap<ByteArray, OpMultimapEntry>();

    LockArray<ByteArray> mutex;

    public OpMultimap() {
        mutex = new LockArray<ByteArray>(N_PARTITIONS);
    }

    /**
     * Add a set of operations to historic
     * 
     * @param key
     * @param values
     */
    public void putAll(ByteArray key, List<Op> values) {
        mutex.lock(key);

        OpMultimapEntry entry = getOrCreate(key);
        entry.addAll(values);

        mutex.release(key);
    }

    public Op getLastWrite(ByteArray key) {
        mutex.lock(key);
        OpMultimapEntry l = map.get(key);
        Op op = null;
        if(l != null) {
            op = l.getLastWrite();
        }
        mutex.release(key);
        return op;
    }

    public OpMultimapEntry get(ByteArray key) {
        mutex.lock(key);

        OpMultimapEntry entry = map.get(key);

        mutex.release(key);
        return entry;
    }

    public OpMultimapEntry put(ByteArray key, Op op) {
        mutex.lock(key);

        OpMultimapEntry l = getOrCreate(key);
        l.addLast(op);

        mutex.release(key);
        return l;
    }

    public void trackAccess(ByteArray key, OpType type, long rid) {
        mutex.lock(key);
        OpMultimapEntry entry = getOrCreate(key);
        mutex.release(key);

        if(type.equals(Op.OpType.Get)) {
            entry.lockRead();
        } else {
            entry.lockWrite();
        }

        mutex.lock(key);
        log.info("TrackAccess " + key + " " + type + " " + rid);
        entry.addLast(new Op(rid, type));

        mutex.release(key);
    }

    public void endAccess(ByteArray key, OpType type) {
        mutex.lock(key);

        OpMultimapEntry l = getOrCreate(key);
        assert (l != null);
        if(type.equals(Op.OpType.Get)) {
            l.unlockRead();
        } else {
            l.unlockWrite();
        }

        mutex.release(key);

    }

    /**
     * Only one thread should renew the map
     * 
     * @return
     */
    public OpMultimapView renew() {
        HashMap<ByteArray, OpMultimapEntry> copy = new HashMap<ByteArray, OpMultimapEntry>();
        mutex.lockAllMutex();
        Iterator<ByteArray> i = map.keySet().iterator();
        while(i.hasNext()) {
            ByteArray k = i.next();
            OpMultimapEntry entry = map.get(k);
            if(entry.hasLocked()) {
                log.debug("Has locked");
                copy.put(k, new OpMultimapEntry(entry.extractAll()));
            } else {
                log.debug("No locked Pendents");
                copy.put(k, map.remove(k));
            }
        }
        mutex.releaseAllMutex();

        return new OpMultimapView(copy);
    }

    /**
     * Get the entry and create if it do not exists
     * 
     * @param key
     * @return
     */
    private OpMultimapEntry getOrCreate(ByteArray key) {
        OpMultimapEntry l = map.get(key);
        if(l == null) {
            l = new OpMultimapEntry();
            map.put(key, l);
        }
        return l;
    }
}
