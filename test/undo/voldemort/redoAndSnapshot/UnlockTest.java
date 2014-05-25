package voldemort.redoAndSnapshot;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

public class UnlockTest {

    OpMultimap db = new OpMultimap();

    ByteArray k1 = new ByteArray("key1".getBytes());

    ByteArray k2 = new ByteArray("key2".getBytes());
    ByteArray k3 = new ByteArray("key3".getBytes());
    DBUndoStub stub;

    List<Thread> tList;

    short branch;
    private boolean contained;
    private long currentCommit;

    @Before
    public void setup() {
        currentCommit = BranchController.INIT_COMMIT;
        branch = BranchController.INIT_BRANCH;
        contained = false;
        stub = new DBUndoStub(true);

        tList = new ArrayList<Thread>();
        tList.add(new ExecOpT(k1.clone(), OpType.Put, stub, db, new RUD(1, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(2, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(3, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Put, stub, db, new RUD(4, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(5, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(6, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Put, stub, db, new RUD(7, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(8, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(9, branch, contained)));
    }

    /**
     * 
     * @throws InterruptedException
     */
    @Test
    public void redoIsolated() throws InterruptedException {
        System.out.println("----- Start test: Unlock Test -------");

        execOperations(false);
        // the database is populated and stub has the operation ordering
        ByteArray kOriginalBranch = stub.modifyKey(k1.clone(), branch, currentCommit);
        System.out.println(db.get(kOriginalBranch));

        System.out.println("--------- Prepare redo -------------");
        assertEquals(tList.size(), db.get(kOriginalBranch).size());
        OpMultimap dbOriginal = db;
        db = new OpMultimap();

        System.out.println("--------- Start redo -------------");
        // create new branch to start the redo
        branch = 1;
        BranchPath redoPath = new BranchPath(new StsBranchPair(0L, 1),
                                             new StsBranchPair(0L, 0),
                                             new StsBranchPair(0L, 1));
        stub.newRedo(redoPath);

        tList = new ArrayList<Thread>();
        int i = 1;
        tList.add(new ExecOpT(k1.clone(), OpType.Put, stub, db, new RUD(1, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(2, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(3, branch, contained)));
        execOperations(true);
        tList.clear();

        // unlock the put

        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(5, branch, contained)));
        // I
        tList.add(new ExecOpT(k1.clone(), OpType.Put, stub, db, new RUD(7, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(8, branch, contained)));

        tList.add(new ExecOpT(k1.clone(), OpType.Get, stub, db, new RUD(9, branch, contained)));
        tList.add(new ExecOpT(k1.clone(), OpType.UNLOCK, stub, db, new RUD(4, branch, contained)));

        tList.add(new ExecOpT(k1.clone(), OpType.UNLOCK, stub, db, new RUD(6, branch, contained)));

        execOperations(true);

        System.out.println("--------- Redo over, compare now -------------");
        // Check result: same order in re-execution
        ArrayList<Op> originalEntry = dbOriginal.get(kOriginalBranch).getAll();
        ByteArray kNewBranch = stub.modifyKey(k1.clone(), branch, currentCommit);
        ArrayList<Op> newEntry = db.get(kNewBranch).getAll();
        int k = 0;
        List<Integer> unlocked = Arrays.asList(4, 6);
        for(i = 0; i < newEntry.size(); i++) {
            if(unlocked.contains(i + 1)) {
                k++;
                // ignore the unlocked
                continue;
            }
            assertEquals(originalEntry.get(i).type, newEntry.get(i - k).type);
        }
    }

    void execOperations(boolean parallel) throws InterruptedException {
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
