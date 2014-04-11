package voldemort.redoAndSnapshot;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

/**
 * Base test unit to simulate database access
 * 
 * @author darionascimento
 * 
 */
public class ExecOpT extends Thread {

    Op op;
    DBUndoStub scheduler;
    private ByteArray key;
    private OpMultimap db;
    private boolean contained;
    private short branch;

    public ExecOpT(ByteArray key,
                   long rid,
                   OpType type,
                   DBUndoStub stub,
                   OpMultimap db,
                   boolean contained,
                   int branch) {
        super();
        this.op = new Op(rid, type);
        this.scheduler = stub;
        this.key = key;
        this.db = db;
        this.contained = contained;
        this.branch = (short) branch;
    }

    @Override
    public void run() {
        exec();
    }

    /**
     * Exec the command (call .start() for assync or exec to sync)
     */
    public void exec() {
        try {
            switch(op.type) {
                case Delete:
                    System.out.println("Try Delete: " + op.rid);
                    scheduler.deleteStart(key, op.rid, branch, contained);
                    db.put(key.clone(), op);
                    System.out.println("Deleting...: " + op.rid);
                    sleep(1000);
                    scheduler.deleteEnd(key, op.rid, branch, contained);
                    System.out.println("Deleted: " + op.rid);
                    break;
                case Put:
                    System.out.println("Try put: " + op.rid);
                    scheduler.putStart(key, op.rid, branch, contained);
                    db.put(key.clone(), op);
                    System.out.println("Putting...: " + op.rid);
                    sleep(1000);
                    scheduler.putEnd(key, op.rid, branch, contained);
                    System.out.println("putted: " + op.rid);
                    break;
                case Get:
                    System.out.println("Try get: " + op.rid);
                    scheduler.getStart(key, op.rid, branch, contained);
                    db.put(key.clone(), op);
                    System.out.println("getting...: " + op.rid);
                    sleep(1000);
                    scheduler.getEnd(key, op.rid, branch, contained);
                    System.out.println("getted: " + op.rid);
                    break;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
