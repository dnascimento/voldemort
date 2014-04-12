package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
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
     * @paramrud
     * @param sts
     * @param branch
     * @return the key version to access
     */
    @Override
    public long getStart(ByteArray key, RUD rud, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Get, rud, sts);
    }

    @Override
    public long putStart(ByteArray key, RUD rud, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Put, rud, sts);
    }

    @Override
    public long deleteStart(ByteArray key, RUD rud, long sts) {
        return keyAccessLists.trackAccess(key, Op.OpType.Delete, rud, sts);
    }

    @Override
    public void getEnd(ByteArray key, RUD rud) {
        keyAccessLists.endAccess(key, OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, RUD rud) {
        keyAccessLists.endAccess(key, OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, RUD rud) {
        keyAccessLists.endAccess(key, OpType.Delete);
    }

    @Override
    public long getVersionStart(ByteArray key, RUD rud, long sts) {
        return keyAccessLists.getVersionToPut(key, rud, sts);
    }

}
