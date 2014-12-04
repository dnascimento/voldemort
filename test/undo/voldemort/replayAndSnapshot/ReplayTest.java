package voldemort.replayAndSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.KeyMapEntry;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.VersionShuttle;
import voldemort.utils.ByteArray;

public class ReplayTest {

    KeyMap db = new KeyMap();

    ByteArray k1 = new ByteArray("key1".getBytes());

    ByteArray k2 = new ByteArray("key2".getBytes());
    ByteArray k3 = new ByteArray("key3".getBytes());
    DBProxy stub;

    List<Thread> tList;
    LinkedList<OpType> order;

    short branch;
    private boolean contained;
    private long currentCommit;

    @Before
    public void setup() {
        currentCommit = BranchController.INIT_COMMIT;
        branch = BranchController.INIT_BRANCH;
        contained = false;

        order = new LinkedList<OpType>();
        List<Op> l = new ArrayList<Op>();
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Get);
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Get);
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Get);

        for(int i = 0; i < order.size(); i++) {
            OpType t = order.get(i);
            l.add(new Op(i + 1, t));
        }
        tList = new ArrayList<Thread>();
    }

    /**
     * Re-execution of actions set.
     * Execute a set of serialized actions (non-parallel). Then, re-execute this
     * actions in a new branch and compare the branches. The order must be
     * equivalent.
     * 
     * @throws InterruptedException
     */
    @Test
    public void replayIsolated() throws InterruptedException {
        System.out.println("----- Start test: Replay Isolated -------");

        stub = new DBProxy();

        execOperations(false);
        // the database is populated and stub has the operation ordering
        ByteArray kOriginalBranch = stub.modifyKey(k1.clone(), branch, currentCommit);
        System.out.println(db.get(kOriginalBranch));

        System.out.println("--------- Prepare replay -------------");
        assertEquals(order.size(), db.get(kOriginalBranch).size());
        KeyMap dbOriginal = db;
        db = new KeyMap();

        System.out.println("--------- Start replay -------------");
        // create new branch to start the replay
        branch = 1;
        BranchPath replayPath = new BranchPath(new VersionShuttle(0L, 1),
                                               new VersionShuttle(0L, 0),
                                               new VersionShuttle(0L, 1));
        stub.newReplay(replayPath);

        execOperations(true);

        // Check result: same order in re-execution
        ArrayList<Op> originalEntry = dbOriginal.get(kOriginalBranch).operationList;
        ByteArray kNewBranch = stub.modifyKey(k1.clone(), branch, currentCommit);
        ArrayList<Op> newEntry = db.get(kNewBranch).operationList;
        for(int i = 0; i < originalEntry.size(); i++) {
            assertEquals(originalEntry.get(i).type, newEntry.get(i).type);
        }
    }

    @Test
    public void restrain() throws InterruptedException {
        System.out.println("----- Start test: Restrain -------");

        stub = new DBProxy();
        BranchPath replayPath = new BranchPath(new VersionShuttle(0L, 1),
                                               new VersionShuttle(0L, 0),
                                               new VersionShuttle(0L, 1));
        stub.newReplay(replayPath);

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(1, 0, false)).exec();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(2, 0, false)).exec();
        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(3, 0, false)).exec();

        // replay
        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(1, 1, false)).exec();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(4, 0, false)).exec();

        // replay
        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(2, 1, false)).exec();
        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(3, 1, false)).exec();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(5, 0, false)).exec();

        // replay
        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(4, 1, false)).exec();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(6, 0, false)).exec();
        // restrain 7:2

        Thread t1 = new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(7, 1, true));
        t1.start();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(5, 1, false)).exec();

        // restrain 8:2
        Thread t2 = new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(8, 1, true));
        t2.start();

        new ExecOpT(k1.clone(), OpType.Put, stub, db, new SRD(6, 1, false)).exec();

        stub.replayOver();

        t1.join();
        t2.join();
        ByteArray k1Branch2Snaphost0 = stub.modifyKey(k1.clone(), (short) 1, 0);
        // the restrain operations should be in the branch 2
        KeyMapEntry entryNewBranch = db.get(k1Branch2Snaphost0);
        assertTrue(entryNewBranch != null);
        System.out.println(entryNewBranch);
        assertEquals(8, entryNewBranch.operationList.size());
    }

    void execOperations(boolean parallel) throws InterruptedException {
        List<Op> opList = new ArrayList<Op>();

        tList.clear();
        for(int i = 0; i < order.size(); i++) {
            OpType t = order.get(i);
            Op o = new Op(i + 1, t);
            opList.add(o);
            tList.add(new ExecOpT(k1.clone(), t, stub, db, new SRD(i + 1, branch, contained)));
        }

        // Populate the stub to set the ordering in archive
        for(Thread t: tList) {
            t.start();
            if(!parallel)
                t.join();
        }
        if(parallel) {
            for(Thread t: tList) {
                t.join();
            }
        }
    }
}