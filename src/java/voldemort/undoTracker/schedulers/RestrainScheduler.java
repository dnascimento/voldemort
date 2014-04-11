package voldemort.undoTracker.schedulers;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RestrainScheduler implements AccessSchedule {

    private final Logger log = LogManager.getLogger("RestrainScheduler");

    OpMultimap archive;
    private AtomicBoolean flag;

    public RestrainScheduler(OpMultimap archive, AtomicBoolean containingFlag) {
        super();
        this.archive = archive;
        this.flag = containingFlag;
    }

    @Override
    public void getEnd(ByteArray key, long rid) {}

    @Override
    public void putEnd(ByteArray key, long rid) {}

    @Override
    public void deleteEnd(ByteArray key, long rid) {}

    @Override
    public long getStart(ByteArray key, long rid, long sts) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        return 0;
    }

    @Override
    public long putStart(ByteArray key, long rid, long sts) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        return 0;
    }

    @Override
    public long deleteStart(ByteArray key, long rid, long sts) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        return 0;
    }

}
