package voldemort.replayAndSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.KeyMapEntry;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

public class ReplayTest {

    ByteArray k1 = new ByteArray("key1".getBytes());
    ByteArray k2 = new ByteArray("key2".getBytes());
    ByteArray k3 = new ByteArray("key3".getBytes());

    DBProxy proxy;

    List<Thread> threads;
    LinkedList<Op> baseOperations;

    private long timestamp;
    private short branch = BranchController.ROOT_BRANCH;

    @Before
    public void setup() {
        resetTimestamp();
        proxy = new DBProxy();
        branch = BranchController.ROOT_BRANCH;

        baseOperations = new LinkedList<Op>();
        baseOperations.add(new Op(1, OpType.Put));
        baseOperations.add(new Op(2, OpType.Get));
        baseOperations.add(new Op(3, OpType.Get));
        baseOperations.add(new Op(4, OpType.Put));
        baseOperations.add(new Op(5, OpType.Get));
        baseOperations.add(new Op(6, OpType.Get));
        baseOperations.add(new Op(7, OpType.Put));
        baseOperations.add(new Op(8, OpType.Get));
        baseOperations.add(new Op(9, OpType.Get));
        threads = new ArrayList<Thread>();
    }

    @Test
    public void checkOperationList() throws InterruptedException {
        System.out.println("----- Start test: Replay Isolated -------");
        execOperations(k1.clone(),
                       false,
                       BranchController.ROOT_BRANCH,
                       false,
                       new FakeDB(),
                       baseOperations);
        // the database is populated and proxy has the operation ordering
        KeyMapEntry entry = proxy.keyMap.get(k1.clone());
        Iterator<Op> it = baseOperations.iterator();
        for(Op op: entry.operationList) {
            assertEquals(op.toType(), it.next().toType());
        }
    }

    /**
     * Re-execution of actions set.
     * Execute a serie of actions and replay them.
     * Then, compare the branches.
     * 
     * @throws InterruptedException
     */
    @Test
    public void replaySerial() throws InterruptedException {
        FakeDB db = new FakeDB();
        System.out.println("----- Start test: Replay Isolated -------");
        execOperations(k1, false, BranchController.ROOT_BRANCH, false, db, baseOperations);
        // the database is populated and proxy has the operation ordering
        System.out.println(proxy.keyMap.get(k1.clone()));

        System.out.println("--------- Prepare replay -------------");
        // create new branch to start the replay

        BranchPath replayPath = newBranch();
        proxy.newReplay(replayPath);

        FakeDB replayDB = new FakeDB();
        System.out.println("--------- Start replay -------------");
        execOperations(k1, true, branch, false, replayDB, baseOperations);

        System.out.println(db);
        System.out.println(replayDB);
        // Check result: same order in re-execution
        check(k1, db, replayDB, replayPath.latestVersion.sid, new ArrayList<Long>());
    }

    private BranchPath newBranch() {
        long currentTimestamp = getTimestamp();
        short branch = incrementBranch();
        return new BranchPath(branch,
                              currentTimestamp,
                              Arrays.asList(currentTimestamp, BranchController.ROOT_SNAPSHOT));
    }

    /**
     * 
     * @throws InterruptedException
     */
    @Test
    public void unlockTest() throws InterruptedException {
        System.out.println("----- Start test: Unlock Test -------");
        FakeDB db = new FakeDB();
        execOperations(k1, false, BranchController.ROOT_BRANCH, false, db, baseOperations);

        // the database is populated and stub has the operation ordering

        System.out.println("--------- Prepare replay -------------");
        FakeDB replayDB = new FakeDB();
        BranchPath replayPath = newBranch();
        proxy.newReplay(replayPath);

        System.out.println("--------- Start replay -------------");
        // create new branch to start the replay

        execOperations(k1, false, replayPath.branch, false, replayDB, baseOperations.subList(0, 3));
        ArrayList<Op> operations = new ArrayList<Op>();
        // unlock the put
        operations.add(new Op(5, OpType.Get));
        operations.add(new Op(7, OpType.Put));
        operations.add(new Op(8, OpType.Get));
        operations.add(new Op(9, OpType.Get));
        operations.add(new Op(4, OpType.UNLOCK));
        operations.add(new Op(6, OpType.UNLOCK));
        execOperations(k1, true, replayPath.branch, false, replayDB, operations);
        System.out.println("--------- Replay over, compare now -------------");
        // Check result: same order in re-execution
        List<Long> unlocked = Arrays.asList(4L, 6L);
        check(k1, db, replayDB, replayPath.latestVersion.sid, unlocked);

    }

    @Test
    public void restrain() throws InterruptedException {
        System.out.println("----- Start test: Unlock Test -------");
        FakeDB db = new FakeDB();
        execOperations(k1, false, BranchController.ROOT_BRANCH, false, db, baseOperations);

        // the database is populated and stub has the operation ordering

        System.out.println("--------- Prepare replay -------------");
        FakeDB replayDB = new FakeDB();
        BranchPath replayPath = newBranch();
        proxy.newReplay(replayPath);

        System.out.println("--------- Start replay -------------");
        // create new branch to start the replay

        execOperations(k1, false, replayPath.branch, false, replayDB, baseOperations.subList(0, 3));
        Thread t1 = new ExecOpT(k1.clone(), proxy, OpType.Put, branch, true, replayDB, 100l);
        t1.start();
        Thread.sleep(5000);
        // its still restraining
        assertTrue(t1.isAlive());
        execOperations(k1, false, replayPath.branch, false, replayDB, baseOperations.subList(3, 8));
        proxy.replayOver();
        Thread.sleep(2000);
        assertFalse(t1.isAlive());
    }

    private void check(ByteArray key,
                       FakeDB db,
                       FakeDB replayDB,
                       long versionInReplayDB,
                       List<Long> unlocked) {
        // Check number of data items
        // TODO: the same RID may have unlocked various operations
        assertEquals(db.multimap.size() - unlocked.size(), replayDB.multimap.size());

        List<Op> original = db.multimap.get(DBProxy.modifyKey(key.clone(),
                                                              BranchController.ROOT_SNAPSHOT));
        List<Op> replay = replayDB.multimap.get(DBProxy.modifyKey(key.clone(), versionInReplayDB));

        // check number of operations on database item
        assertEquals(original.size() - unlocked.size(), replay.size());
        Iterator<Op> originalIt = original.iterator();
        Iterator<Op> replayIt = replay.iterator();
        while(originalIt.hasNext()) {
            Op o = originalIt.next();
            if(!unlocked.contains(o.rid)) {
                Op r = replayIt.next();
                assertEquals(o.type, r.type);
            }
        }
    }

    void execOperations(ByteArray key,
                        boolean parallel,
                        short branch,
                        boolean restrain,
                        FakeDB db,
                        Op operation) throws InterruptedException {
        execOperations(key, parallel, branch, restrain, db, Arrays.asList(operation));
    }

    void execOperations(ByteArray key,
                        boolean parallel,
                        short branch,
                        boolean restrain,
                        FakeDB db,
                        List<Op> operations) throws InterruptedException {
        threads.clear();
        key = key.clone();
        if(parallel) {
            for(Op op: operations) {
                threads.add(new ExecOpT(key.clone(), proxy, Arrays.asList(op), branch, restrain, db));
            }
        } else {
            threads.add(new ExecOpT(key.clone(), proxy, operations, branch, restrain, db));

        }

        for(Thread t: threads) {
            t.start();
        }
        for(Thread t: threads) {
            t.join();
        }
    }

    private void resetTimestamp() {
        timestamp = 1L;
    }

    private long getTimestamp() {
        return timestamp++;
    }

    private short incrementBranch() {
        return ++branch;
    }

}
