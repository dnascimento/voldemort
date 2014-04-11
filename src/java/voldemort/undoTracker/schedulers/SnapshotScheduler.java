package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

/**
 * @author darionascimento
 * 
 */
public class SnapshotScheduler implements AccessSchedule {

    private OpMultimap keyAccessLists;

    /**
     * Each request handler has a UndoStub instance
     */
    public SnapshotScheduler(OpMultimap keyAccessLists) {
        this.keyAccessLists = keyAccessLists;
    }

    /**
     * 
     * @param key
     * @param rid
     * @param sts
     * @param branch
     * @return the key version to access
     */
    @Override
    public long getStart(ByteArray key, long rid, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Get, rid, sts);
    }

    @Override
    public long putStart(ByteArray key, long rid, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Put, rid, sts);
    }

    @Override
    public long deleteStart(ByteArray key, long rid, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Delete, rid, sts);
    }

    @Override
    public void getEnd(ByteArray key, long rid) {
        keyAccessLists.endAccess(key, OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, long rid) {
        keyAccessLists.endAccess(key, OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, long rid) {
        keyAccessLists.endAccess(key, OpType.Delete);
    }

}
