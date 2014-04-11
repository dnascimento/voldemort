package voldemort.redoAndSnapshot;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.utils.ByteArray;

public class SnapshotTesting {

    ByteArray k1 = new ByteArray("key1".getBytes());
    short branch = 1;
    private boolean contained = false;

    /**
     * Read always the most recent version if RID > STS
     */
    @Test
    public void testSnapshotRead() {

        DBUndoStub stub = new DBUndoStub();
        stub.setNewSnapshotRid(0);
        stub.putStart(k1, 1, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 1, branch, contained);

        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, 2, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 2, branch, contained);

        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, 3, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.getEnd(k1, 3, branch, contained);

        k1 = new ByteArray("key1".getBytes());
        stub.deleteStart(k1, 4, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.deleteEnd(k1, 4, branch, contained);
    }

    /**
     * Test put in new and in old versions and get after. Goal: test if the
     * snapshot splits the values
     */
    @Test
    public void testSnap() {
        DBUndoStub stub = new DBUndoStub();
        stub.setNewSnapshotRid(200);// schedule a snapshot

        // write before snapshot
        stub.putStart(k1, 100, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 100, branch, contained);

        // Write after snapshot (Access new version)
        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, 300, branch, contained);
        assertEquals(200, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 200, branch, contained);

        // read old version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, 101, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.getEnd(k1, 101, branch, contained);

        // read new version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, 301, branch, contained);
        assertEquals(200, DBUndoStub.getKeySnapshot(k1));
        stub.getEnd(k1, 301, branch, contained);

        // Test overwrites
        // write before snapshot
        stub.putStart(k1, 102, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 102, branch, contained);

        // Write after snapshot (Access new version)
        k1 = new ByteArray("key1".getBytes());
        stub.putStart(k1, 302, branch, contained);
        assertEquals(200, DBUndoStub.getKeySnapshot(k1));
        stub.putEnd(k1, 302, branch, contained);

        // read old version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, 103, branch, contained);
        assertEquals(0, DBUndoStub.getKeySnapshot(k1));
        stub.getEnd(k1, 103, branch, contained);

        // read new version
        k1 = new ByteArray("key1".getBytes());
        stub.getStart(k1, 303, branch, contained);
        assertEquals(200, DBUndoStub.getKeySnapshot(k1));
        stub.getEnd(k1, 303, branch, contained);
    }

}
