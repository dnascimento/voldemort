package voldemort.redoAndSnapshot;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.RUD;
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
    private RUD rud;

    public ExecOpT(ByteArray key, RUD rud, OpType type, DBUndoStub stub, OpMultimap db) {
        super();
        this.op = new Op(rud.rid, type);
        this.scheduler = stub;
        this.key = key;
        this.db = db;
        this.rud = rud;
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
                    scheduler.deleteStart(key, new RUD(op.rid, rud.branch, rud.restrain));
                    db.put(key.clone(), op);
                    System.out.println("Deleting...: " + op.rid);
                    sleep(1000);
                    scheduler.deleteEnd(key, new RUD(op.rid, rud.branch, rud.restrain));
                    System.out.println("Deleted: " + op.rid);
                    break;
                case Put:
                    System.out.println("Try put: " + op.rid);
                    scheduler.putStart(key, new RUD(op.rid, rud.branch, rud.restrain));
                    db.put(key.clone(), op);
                    System.out.println("Putting...: " + op.rid);
                    sleep(1000);
                    scheduler.putEnd(key, new RUD(op.rid, rud.branch, rud.restrain));
                    System.out.println("putted: " + op.rid);
                    break;
                case Get:
                    System.out.println("Try get: " + op.rid);
                    scheduler.getStart(key, new RUD(op.rid, rud.branch, rud.restrain));
                    db.put(key.clone(), op);
                    System.out.println("getting...: " + op.rid);
                    sleep(1000);
                    scheduler.getEnd(key, new RUD(op.rid, rud.branch, rud.restrain));
                    System.out.println("getted: " + op.rid);
                    break;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
