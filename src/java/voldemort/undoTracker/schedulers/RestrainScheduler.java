package voldemort.undoTracker.schedulers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RestrainScheduler implements AccessSchedule {

    private final Logger log = LogManager.getLogger(RestrainScheduler.class.getName());

    OpMultimap archive;
    private Object flag;

    public RestrainScheduler(OpMultimap archive, Object restrainLocker) {
        super();
        this.archive = archive;
        this.flag = restrainLocker;
    }

    @Override
    public void getEnd(ByteArray key, RUD rud) {}

    @Override
    public void putEnd(ByteArray key, RUD rud) {}

    @Override
    public void deleteEnd(ByteArray key, RUD rud) {}

    @Override
    public long getStart(ByteArray key, RUD rud, long sts) {
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
    public long putStart(ByteArray key, RUD rud, long sts) {
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
    public long deleteStart(ByteArray key, RUD rud, long sts) {
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
    public long getVersionStart(ByteArray clone, RUD rud, long sts) {
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
