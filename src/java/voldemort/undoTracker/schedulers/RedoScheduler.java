package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RedoScheduler implements AccessSchedule {

    OpMultimap archive;

    public RedoScheduler(OpMultimap archive) {
        super();
        this.archive = archive;
    }

    @Override
    public long getStart(ByteArray key, long rid, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextGet(rid);
        return sts;
    }

    @Override
    public long putStart(ByteArray key, long rid, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextPut(rid);
        return sts;
    }

    @Override
    public long deleteStart(ByteArray key, long rid, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextDelete(rid);
        return sts;
    }

    @Override
    public void getEnd(ByteArray key, long rid) {
        archive.get(key).endOp(OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, long rid) {
        archive.get(key).endOp(OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, long rid) {
        archive.get(key).endOp(OpType.Delete);
    }
}
