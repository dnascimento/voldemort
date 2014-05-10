package voldemort.redoAndSnapshot;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchController;
import voldemort.utils.ByteArray;

public class CommitTesting {

    ByteArray k1 = new ByteArray("key1".getBytes());
    short branch = BranchController.INIT_BRANCH;
    long init_commit = BranchController.INIT_COMMIT;
    private boolean contained = false;

    /**
     * Read always the most recent version if rud > STS
     */
    @Test
    public void testCommitRead() {
        DBUndoStub stub = new DBUndoStub(true);
        stub.putStart(k1, new RUD(1, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(1, branch, contained));

        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, new RUD(2, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(2, branch, contained));

        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, new RUD(3, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.getEnd(k1, new RUD(3, branch, contained));

        k1 = new ByteArray("key1".getBytes());
        stub.deleteStart(k1, new RUD(4, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.deleteEnd(k1, new RUD(4, branch, contained));
    }

    /**
     * Test put in new and in old versions and get after. Goal: test if the
     * commit splits the values
     */
    @Test
    public void testSnap() {

        DBUndoStub stub = new DBUndoStub(true);
        stub.scheduleNewCommit(200);// schedule a commit

        // write before commit
        stub.putStart(k1, new RUD(100, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(100, branch, contained));

        // Write after commit (Access new version)
        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, new RUD(300, branch, contained));
        assertEquals(200, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(200, branch, contained));

        // read old version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, new RUD(101, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.getEnd(k1, new RUD(101, branch, contained));

        // read new version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, new RUD(301, branch, contained));
        assertEquals(200, stub.getKeyCommit(k1));
        stub.getEnd(k1, new RUD(301, branch, contained));

        // Test overwrites
        // write before commit
        stub.putStart(k1, new RUD(102, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(102, branch, contained));

        // Write after commit (Access new version)
        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, new RUD(302, branch, contained));
        assertEquals(200, stub.getKeyCommit(k1));
        stub.putEnd(k1, new RUD(302, branch, contained));

        // read old version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, new RUD(103, branch, contained));
        assertEquals(init_commit, stub.getKeyCommit(k1));
        stub.getEnd(k1, new RUD(103, branch, contained));

        // read new version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, new RUD(303, branch, contained));
        assertEquals(200, stub.getKeyCommit(k1));
        stub.getEnd(k1, new RUD(303, branch, contained));
    }

}
