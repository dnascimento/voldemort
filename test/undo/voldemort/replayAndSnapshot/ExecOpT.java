package voldemort.replayAndSnapshot;

import java.util.Arrays;
import java.util.List;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

/**
 * Base test unit to simulate database access
 * 
 * @author darionascimento
 * 
 */
public class ExecOpT extends Thread {

    List<Op> operations;
    DBProxy proxy;
    private ByteArray key;
    private short branch;
    private boolean restrain;
    private FakeDB db;

    public ExecOpT(ByteArray key,
                   DBProxy proxy,
                   OpType type,
                   short branch,
                   boolean restrain,
                   FakeDB db,
                   long rid) {
        this(key, proxy, Arrays.asList(new Op(rid, type)), branch, restrain, db);
    }

    public ExecOpT(ByteArray key,
                   DBProxy proxy,
                   List<Op> operations,
                   short branch,
                   boolean restrain,
                   FakeDB db) {
        super();
        this.operations = operations;
        this.proxy = proxy;
        this.key = key;
        this.branch = branch;
        this.restrain = restrain;
        this.db = db;
    }

    @Override
    public void run() {
        try {
            exec();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Exec the command (call .start() for assync or exec to sync)
     * 
     * @throws InterruptedException
     */
    public void exec() throws InterruptedException {
        for(Op op: operations) {
            SRD srd = new SRD(op.rid, branch, restrain);
            OpType type = op.toType();

            if(type.equals(OpType.UNLOCK)) {
                System.out.println("Try to Unlock: " + op.rid);
                proxy.unlockKeys(Arrays.asList(key), srd);
                System.out.println("unlocked: " + op.rid);
            } else {
                System.out.println("start " + type + ": " + op.rid);
                proxy.startOperation(op.toType(), key, srd);
                System.out.println(type + "ing...: " + op.rid);
                db.newOperation(key, op);
                sleep(1000);
                proxy.endOperation(type, key, srd);
                System.out.println(type + "ed: " + op.rid);
            }
        }
    }
}
