package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.InvertDependencies;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

public class CurrentSnapshotScheduler implements AccessSchedule {

    private static OpMultimap keyAccessLists;
    private static InvertDependencies sender;

    private synchronized void init(OpMultimap archive) {
        if(sender != null)
            return;
        keyAccessLists = new OpMultimap();
        sender = new InvertDependencies(keyAccessLists, archive);
        sender.start();
    }

    /**
     * Each request handler has a UndoStub instance
     */
    public CurrentSnapshotScheduler(OpMultimap archive) {
        if(sender == null)
            init(archive);
    }

    @Override
    public void getStart(ByteArray key, long rid) {
        if(rid != 0) {
            keyAccessLists.trackAccess(key, Op.OpType.Get, rid);
        }
    }

    @Override
    public void putStart(ByteArray key, long rid) {
        if(rid != 0) {
            keyAccessLists.trackAccess(key, Op.OpType.Put, rid);
        }
    }

    @Override
    public void deleteStart(ByteArray key, long rid) {
        if(rid != 0) {
            keyAccessLists.trackAccess(key, Op.OpType.Delete, rid);
        }
    }

    @Override
    public void getEnd(ByteArray key, long rid) {
        if(rid != 0)
            keyAccessLists.endAccess(key, OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, long rid) {
        if(rid != 0)
            keyAccessLists.endAccess(key, OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, long rid) {
        if(rid != 0)
            keyAccessLists.endAccess(key, OpType.Delete);
    }
}
