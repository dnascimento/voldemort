package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
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
    public long getStart(ByteArray key, RUD rud, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextGet(rud);
        return sts;
    }

    @Override
    public long putStart(ByteArray key, RUD rud, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextPut(rud);
        return sts;
    }

    @Override
    public long deleteStart(ByteArray key, RUD rud, long sts) {
        OpMultimapEntry l = archive.get(key);
        l.isNextDelete(rud);
        return sts;
    }

    @Override
    public void getEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Delete);
    }
}
