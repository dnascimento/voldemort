package voldemort.undoTracker.map;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultimap;

/**
 * A HashMap where same items are append to a list and each entry has a
 * read/write locker to schedule the access
 * 
 * @author darionascimento
 * 
 */
public class OpMultimap {

    private final Logger log = LogManager.getLogger("OpMultimap");
    /**
     * The hash table
     */
    private ConcurrentHashMap<ByteArray, OpMultimapEntry> map = new ConcurrentHashMap<ByteArray, OpMultimapEntry>();

    /**
     * Add a set of operations to historic
     * 
     * @param key
     * @param values
     */
    public void putAll(ByteArray key, List<Op> values) {
        System.out.println("PutAll");
        OpMultimapEntry entry = getOrCreate(key);
        entry.addAll(values);
    }

    public Op getLastWrite(ByteArray key) {
        OpMultimapEntry l = map.get(key);
        Op op = null;
        if(l != null) {
            op = l.getLastWrite();
        }
        return op;
    }

    public OpMultimapEntry get(ByteArray key) {
        OpMultimapEntry entry = map.get(key);
        return entry;
    }

    public OpMultimapEntry put(ByteArray key, Op op) {
        OpMultimapEntry l = getOrCreate(key);
        assert (l != null);
        l.addLast(op);
        return l;
    }

    /**
     * Access tracking
     * 
     * @param key
     * @param type
     * @param rid
     * @param sts
     * @return
     */
    public long trackAccess(ByteArray key, OpType type, long rid, long sts) {
        OpMultimapEntry entry = getOrCreate(key);
        // TODO tem de haver um lock para redo e para nao redo, certo?
        if(type.equals(Op.OpType.Get)) {
            entry.lockRead();
        } else {
            entry.lockWrite();
        }

        return entry.trackAccessNewRequest(type, rid, sts);
    }

    public void endAccess(ByteArray key, OpType type) {
        OpMultimapEntry l = getOrCreate(key);
        assert (l != null);
        if(type.equals(Op.OpType.Get)) {
            l.unlockRead();
        } else {
            l.unlockWrite();
        }
    }

    /**
     * Get the entry and create if it do not exists
     * 
     * @param key
     * @return
     */
    private OpMultimapEntry getOrCreate(ByteArray key) {
        OpMultimapEntry entry = map.get(key);
        if(entry == null) {
            entry = map.putIfAbsent(key, new OpMultimapEntry());
            if(entry == null) {
                entry = map.get(key);
            }
        }
        return entry;
    }

    public Set<ByteArray> getKeySet() {
        return map.keySet();
    }

    public void updateDependencies(HashMultimap<Long, Long> dependencyPerRid) {
        for(ByteArray key: getKeySet()) {
            OpMultimapEntry entry = map.get(key);
            assert (entry != null);
            entry.updateDependencies(dependencyPerRid);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        OpMultimap other = (OpMultimap) obj;
        if(map == null) {
            if(other.map != null)
                return false;
        } else if(!map.equals(other.map))
            return false;
        return true;
    }

    public int size() {
        return map.size();
    }

}
