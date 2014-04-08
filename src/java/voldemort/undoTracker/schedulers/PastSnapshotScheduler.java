package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

public class PastSnapshotScheduler implements AccessSchedule {

    OpMultimap archive;

    public PastSnapshotScheduler(OpMultimap archive) {
        super();
        this.archive = archive;
    }

    @Override
    public void getStart(ByteArray key, long rid) {
        OpMultimapEntry l = archive.get(key);
        l.isNextGet(rid);
    }

    @Override
    public void putStart(ByteArray key, long rid) {
        OpMultimapEntry l = archive.get(key);
        l.isNextPut(rid);
    }

    @Override
    public void deleteStart(ByteArray key, long rid) {
        OpMultimapEntry l = archive.get(key);
        l.isNextDelete(rid);
    }

    @Override
    public void getEnd(ByteArray key, long rid) {
        opEnd(key, rid);
    }

    @Override
    public void putEnd(ByteArray key, long rid) {
        opEnd(key, rid);
    }

    @Override
    public void deleteEnd(ByteArray key, long rid) {
        opEnd(key, rid);
    }

    private void opEnd(ByteArray key, long rid) {
        OpMultimapEntry l = archive.get(key);
        if(!l.isEmpty())
            l.remove(rid);
    }

}
