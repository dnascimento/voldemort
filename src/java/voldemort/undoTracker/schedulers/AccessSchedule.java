package voldemort.undoTracker.schedulers;

import voldemort.utils.ByteArray;

public interface AccessSchedule {

    public void getStart(ByteArray key, long rid);

    public void putStart(ByteArray key, long rid);

    public void deleteStart(ByteArray key, long rid);

    public void getEnd(ByteArray key, long rid);

    public void putEnd(ByteArray key, long rid);

    public void deleteEnd(ByteArray key, long rid);
}
