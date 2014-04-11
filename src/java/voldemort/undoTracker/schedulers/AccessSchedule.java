package voldemort.undoTracker.schedulers;

import voldemort.utils.ByteArray;

public interface AccessSchedule {

    public void getEnd(ByteArray key, long rid);

    public void putEnd(ByteArray key, long rid);

    public void deleteEnd(ByteArray key, long rid);

    public long getStart(ByteArray key, long rid, long sts);

    public long putStart(ByteArray key, long rid, long sts);

    public long deleteStart(ByteArray key, long rid, long sts);

}
