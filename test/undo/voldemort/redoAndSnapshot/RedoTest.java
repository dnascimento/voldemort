package voldemort.redoAndSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.utils.ByteArray;

public class RedoTest {

    OpMultimap db = new OpMultimap();

    ByteArray k1 = new ByteArray("key1".getBytes());

    ByteArray k2 = new ByteArray("key2".getBytes());
    ByteArray k3 = new ByteArray("key3".getBytes());
    DBUndoStub stub;

    List<Thread> tList;
    LinkedList<OpType> order;

    short branch = 1;
    private boolean contained = false;
    private long currentCommit = 0;

    @Before
    public void setup() {
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
     * The easiest base test: common requests in current branch and commit
     * Goal: check if possible to exec normal
     * 
     * @throws InterruptedException
     */
    @Test
    public void currentSnasphotAndBranch() throws InterruptedException {
        System.out.println("----- Start test: Current Commit and branch -------");
        stub = new DBUndoStub();
        stub.setNewCommitRid(currentCommit);

        execOperations(false);
        // the database is populated and stub has the operation ordering
        ByteArray k1Versioned = DBUndoStub.modifyKey(k1.clone(), branch, currentCommit);
        System.out.println(db.get(k1Versioned));
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
    public void redoIsolated() throws InterruptedException {
        System.out.println("----- Start test: Redo Isolated -------");

        stub = new DBUndoStub();
        stub.setNewCommitRid(currentCommit);

        execOperations(false);
        // the database is populated and stub has the operation ordering
        ByteArray kOriginalBranch = DBUndoStub.modifyKey(k1.clone(), branch, currentCommit);
        System.out.println(db.get(kOriginalBranch));

        System.out.println("--------- Prepare redo -------------");
        assertEquals(order.size(), db.get(kOriginalBranch).size());
        OpMultimap dbOriginal = db;
        db = new OpMultimap();

        System.out.println("--------- Start redo -------------");
        // create new branch to start the redo
        branch = 2;

        execOperations(true);

        // Check result: same order in re-execution
        ArrayList<Op> originalEntry = dbOriginal.get(kOriginalBranch).getAll();
        ByteArray kNewBranch = DBUndoStub.modifyKey(k1.clone(), branch, currentCommit);
        ArrayList<Op> newEntry = db.get(kNewBranch).getAll();
        for(int i = 0; i < originalEntry.size(); i++) {
            assertEquals(originalEntry.get(i).type, newEntry.get(i).type);
        }
    }

    @Test
    public void restrain() throws InterruptedException {
        System.out.println("----- Start test: Restrain -------");

        stub = new DBUndoStub();

        new ExecOpT(k1.clone(), new RUD(1, (short) 1, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(2, (short) 1, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(3, (short) 1, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(1, (short) 2, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(4, (short) 1, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(2, (short) 2, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(3, (short) 2, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(5, (short) 1, false), OpType.Put, stub, db).exec();
        new ExecOpT(k1.clone(), new RUD(4, (short) 2, false), OpType.Put, stub, db).exec();

        new ExecOpT(k1.clone(), new RUD(6, (short) 1, false), OpType.Put, stub, db).exec();
        // restrain 7:2

        Thread t1 = new ExecOpT(k1.clone(), new RUD(7, (short) 2, true), OpType.Put, stub, db);
        t1.start();

        new ExecOpT(k1.clone(), new RUD(5, (short) 2, false), OpType.Put, stub, db).exec();

        // restrain 8:2
        Thread t2 = new ExecOpT(k1.clone(), new RUD(8, (short) 2, true), OpType.Put, stub, db);
        t2.start();

        new ExecOpT(k1.clone(), new RUD(6, (short) 2, false), OpType.Put, stub, db).exec();

        stub.unlockRestrain((short) 2);

        t1.join();
        t2.join();
        ByteArray k1Branch2Snaphost0 = DBUndoStub.modifyKey(k1.clone(), (short) 2, 1);
        // the restrain operations should be in the branch 2
        OpMultimapEntry entryNewBranch = db.get(k1Branch2Snaphost0);
        assertTrue(entryNewBranch != null);
        System.out.println(entryNewBranch);
        assertEquals(8, entryNewBranch.getAll().size());
    }

    // Aux
    void execOperations(boolean parallel) throws InterruptedException {
        List<Op> opList = new ArrayList<Op>();

        tList.clear();
        for(int i = 0; i < order.size(); i++) {
            OpType t = order.get(i);
            Op o = new Op(i + 1, t);
            opList.add(o);
            tList.add(new ExecOpT(k1.clone(), new RUD(i + 1, branch, contained), t, stub, db));
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
