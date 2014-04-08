package voldemort;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.schedulers.AccessSchedule;
import voldemort.utils.ByteArray;

/**
 * Base test unit to simulate database access
 * 
 * @author darionascimento
 * 
 */
public class ExecOpT extends Thread {

    Op op;
    AccessSchedule scheduler;
    private ByteArray key;
    private OpMultimap db;

    public ExecOpT(ByteArray key, Op op, AccessSchedule scheduler, OpMultimap db) {
        super();
        this.op = op;
        this.scheduler = scheduler;
        this.key = key;
        this.db = db;
    }

    @Override
    public void run() {
        try {
            switch(op.type) {
                case Delete:
                    System.out.println("Try Delete: " + op.rid);
                    scheduler.deleteStart(key, op.rid);
                    db.put(key, op);
                    System.out.println("Deleting...: " + op.rid);
                    sleep(1000);
                    scheduler.deleteEnd(key, op.rid);
                    System.out.println("Deleted: " + op.rid);
                    break;
                case Put:
                    System.out.println("Try put: " + op.rid);
                    scheduler.putStart(key, op.rid);
                    db.put(key, op);
                    System.out.println("Putting...: " + op.rid);
                    sleep(1000);
                    scheduler.putEnd(key, op.rid);
                    System.out.println("putted: " + op.rid);
                    break;
                case Get:
                    System.out.println("Try get: " + op.rid);
                    scheduler.getStart(key, op.rid);
                    db.put(key, op);
                    System.out.println("getting...: " + op.rid);
                    sleep(1000);
                    scheduler.getEnd(key, op.rid);
                    System.out.println("getted: " + op.rid);
                    break;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
