package voldemort.redoAndSnapshot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
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
    DBProxy scheduler;
    private ByteArray key;
    private OpMultimap db;
    private SRD[] ruds;

    public ExecOpT(ByteArray key, OpType type, DBProxy stub, OpMultimap db, SRD... ruds) {
        super();
        ops = new LinkedList<Op>();

        for(SRD r: ruds) {
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
            SRD srd = ruds[i];
            Op op = it.next();
            try {
                switch(op.type) {
                    case Delete:
                        System.out.println("Try Delete: " + op.rid);
                        scheduler.deleteStart(key, new SRD(op.rid, srd.branch, srd.restrain));
                        db.put(key.clone(), op);
                        System.out.println("Deleting...: " + op.rid);
                        sleep(1000);
                        scheduler.deleteEnd(key, new SRD(op.rid, srd.branch, srd.restrain));
                        System.out.println("Deleted: " + op.rid);
                        break;
                    case Put:
                        System.out.println("Try put: " + op.rid);
                        scheduler.putStart(key, new SRD(op.rid, srd.branch, srd.restrain));
                        db.put(key.clone(), op);
                        System.out.println("Putting...: " + op.rid);
                        sleep(1000);
                        scheduler.putEnd(key, new SRD(op.rid, srd.branch, srd.restrain));
                        System.out.println("putted: " + op.rid);
                        break;
                    case Get:
                        System.out.println("Try get: " + op.rid);
                        scheduler.getStart(key, new SRD(op.rid, srd.branch, srd.restrain));
                        db.put(key.clone(), op);
                        System.out.println("getting...: " + op.rid);
                        sleep(1000);
                        scheduler.getEnd(key, new SRD(op.rid, srd.branch, srd.restrain));
                        System.out.println("got: " + op.rid);
                        break;
                    case GetVersion:
                        System.out.println("Try get version: " + op.rid);
                        scheduler.getVersion(key, new SRD(op.rid, srd.branch, srd.restrain));
                        db.put(key.clone(), op);
                        System.out.println("got...: " + op.rid);
                        sleep(1000);
                        scheduler.getEnd(key, new SRD(op.rid, srd.branch, srd.restrain));
                        System.out.println("got: " + op.rid);
                        break;
                    case UNLOCK:
                        System.out.println("Try to Unlock: " + op.rid);
                        scheduler.unlockKey(Arrays.asList(key), new SRD(op.rid,
                                                                        srd.branch,
                                                                        srd.restrain));
                        System.out.println("unlocked: " + op.rid);

                    default:
                        break;
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
