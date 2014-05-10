package voldemort.redoAndSnapshot;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

    List<Op> ops;
    DBUndoStub scheduler;
    private ByteArray key;
    private OpMultimap db;
    private RUD[] ruds;

    public ExecOpT(ByteArray key, OpType type, DBUndoStub stub, OpMultimap db, RUD... ruds) {
        super();
        ops = new LinkedList<Op>();

        for(RUD r: ruds) {
            ops.add(new Op(r.rid, type));
        }
        this.scheduler = stub;
        this.key = key;
        this.db = db;
        this.ruds = ruds;
    }

    @Override
    public void run() {
        exec();
    }

    /**
     * Exec the command (call .start() for assync or exec to sync)
     */
    public void exec() {
        for(int i = 0; i < ruds.length; i++) {
            Iterator<Op> it = ops.iterator();
            RUD rud = ruds[i];
            Op op = it.next();
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
                        System.out.println("got: " + op.rid);
                        break;
                    case GetVersion:
                        System.out.println("Try get version: " + op.rid);
                        scheduler.getVersion(key, new RUD(op.rid, rud.branch, rud.restrain));
                        db.put(key.clone(), op);
                        System.out.println("got...: " + op.rid);
                        sleep(1000);
                        scheduler.getEnd(key, new RUD(op.rid, rud.branch, rud.restrain));
                        System.out.println("got: " + op.rid);
                        break;
                    default:
                        break;
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
