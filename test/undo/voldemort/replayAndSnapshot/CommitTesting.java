package voldemort.replayAndSnapshot;

import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.branching.BranchController;
import voldemort.utils.ByteArray;

public class CommitTesting {

    ByteArray k1 = new ByteArray("key1".getBytes());
    short branch = BranchController.ROOT_BRANCH;
    long init_commit = BranchController.ROOT_SNAPSHOT;
    private boolean contained = false;

    /**
     * Read always the most recent version if srd > STS
     */
    @Test
    public void testCommitRead() {
        // DBProxy stub = new DBProxy();
        // stub.putStart(k1, new SRD(1, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(1, branch, contained));
        //
        // k1 = new ByteArray("key1".getBytes());
        // stub.putStart(k1, new SRD(2, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(2, branch, contained));
        //
        // k1 = new ByteArray("key1".getBytes());
        // stub.getStart(k1, new SRD(3, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.getEnd(k1, new SRD(3, branch, contained));
        //
        // k1 = new ByteArray("key1".getBytes());
        // stub.deleteStart(k1, new SRD(4, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.deleteEnd(k1, new SRD(4, branch, contained));
    }

    /**
     * Test put in new and in old versions and get after. Goal: test if the
     * commit splits the values
     */
    @Test
    public void testSnap() {
        int COMMIT_ID = 200;
        DBProxy stub = new DBProxy();
        stub.scheduleNewSnapshot(COMMIT_ID);// schedule a commit

        // write before commit
        k1 = new ByteArray("key1".getBytes());
        // stub.putStart(k1, new SRD(100, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(100, branch, contained));
        //
        // // Write after commit (Access new version)
        // k1 = new ByteArray("key1".getBytes());
        // stub.putStart(k1, new SRD(300, branch, contained));
        // assertEquals(COMMIT_ID, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(300, branch, contained));
        //
        // // read old version
        // k1 = new ByteArray("key1".getBytes());
        // stub.getStart(k1, new SRD(101, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.getEnd(k1, new SRD(101, branch, contained));
        //
        // // read new version
        // k1 = new ByteArray("key1".getBytes());
        // stub.getStart(k1, new SRD(301, branch, contained));
        // assertEquals(COMMIT_ID, stub.getKeyCommit(k1));
        // stub.getEnd(k1, new SRD(301, branch, contained));
        //
        // // Test overwrites
        // // write before commit
        // stub.putStart(k1, new SRD(102, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(102, branch, contained));
        //
        // // Write after commit (Access new version)
        // k1 = new ByteArray("key1".getBytes());
        // stub.putStart(k1, new SRD(302, branch, contained));
        // assertEquals(COMMIT_ID, stub.getKeyCommit(k1));
        // stub.putEnd(k1, new SRD(302, branch, contained));
        //
        // // read old version
        // k1 = new ByteArray("key1".getBytes());
        // stub.getStart(k1, new SRD(103, branch, contained));
        // assertEquals(init_commit, stub.getKeyCommit(k1));
        // stub.getEnd(k1, new SRD(103, branch, contained));
        //
        // // read new version
        // k1 = new ByteArray("key1".getBytes());
        // stub.getStart(k1, new SRD(303, branch, contained));
        // assertEquals(COMMIT_ID, stub.getKeyCommit(k1));
        // stub.getEnd(k1, new SRD(303, branch, contained));
    }

}
