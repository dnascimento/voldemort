package voldemort;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapView;
import voldemort.utils.ByteArray;

public class UndoTest {

    OpMultimap db = new OpMultimap();

    ByteArray k1 = new ByteArray("key1".getBytes());
    ByteArray k2 = new ByteArray("key2".getBytes());
    ByteArray k3 = new ByteArray("key3".getBytes());
    DBUndoStub stub;
    long rid = 1;

    List<Thread> tList;
    LinkedList<OpType> order;

    @Before
    public void setup() {
        stub = new DBUndoStub();

        order = new LinkedList<OpType>();
        List<Op> l = new ArrayList<Op>();
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Get);
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Delete);
        order.add(OpType.Put);
        order.add(OpType.Get);
        order.add(OpType.Get);

        for(int i = 0; i < order.size(); i++) {
            OpType t = order.get(i);
            l.add(new Op(i + 1, t));
        }

        tList = new ArrayList<Thread>();
    }

    // /**
    // * Current snapshot do not have order. However, there must be mutual
    // * exclusion between read and writes.
    // *
    // * @throws InterruptedException
    // */
    // @Test
    // public void testCurrentSnapshot() throws InterruptedException {
    // for(int i = 0; i < order.size(); i++) {
    // OpType t = order.get(i);
    // Op o = new Op(i + 1, t);
    // tList.add(new ExecOpT(k1, o, stub, db));
    // }
    //
    // // force first op: put
    // Thread put = tList.remove(0);
    // put.start();
    // put.join();
    //
    // for(Thread t: tList) {
    // t.start();
    // }
    // Thread.sleep(10000);
    //
    // for(Thread t: tList) {
    // t.join();
    // }
    // }

    @Test
    public void testPast() throws InterruptedException {
        OpMultimap archive = new OpMultimap();
        List<Op> opList = new ArrayList<Op>();

        // Create archive to force the order
        stub = new DBUndoStub(archive);
        stub.setNewSnapshotRid(order.size() + 20);

        for(int i = 0; i < order.size(); i++) {
            OpType t = order.get(i);
            Op o = new Op(i + 1, t);
            opList.add(o);
            tList.add(new ExecOpT(k1, o, stub, db));
        }
        archive.putAll(k1, opList);

        for(Thread t: tList) {
            t.start();
        }
        Thread.sleep(10000);

        for(Thread t: tList) {
            t.join();
        }

        OpMultimapView view = db.renew();
        List<Op> result = view.get(k1);
        for(int i = 0; i < order.size(); i++) {
            assertEquals(order.get(i), result.get(i).type);
        }
    }
}
