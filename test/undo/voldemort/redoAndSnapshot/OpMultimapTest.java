package voldemort.redoAndSnapshot;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

public class OpMultimapTest extends Thread {

    ByteArray key = new ByteArray("batatas".getBytes());
    StsBranchPair current = new StsBranchPair(00L, 00);
    BranchPath path = new BranchPath(current, current);
    private OpMultimap map;
    private RUD ignoreRud;

    LinkedList<Op> expected;
    LinkedList<Op> execution;
    Op delayedOp;

    @Before
    public void prepare() {
        expected = new LinkedList<Op>();
        execution = new LinkedList<Op>();
        map = new OpMultimap();
        ignoreRud = null;
    }

    // @Test
    // public void simulation() {
    // map.trackWriteAccess(key, OpType.Put, new RUD(1000L, 0, false), path);
    // expected.add(new Op(1000L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // map.trackWriteAccess(key, OpType.Put, new RUD(1001L, 0, false), path);
    // expected.add(new Op(1001L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // map.trackWriteAccess(key, OpType.Put, new RUD(1002L, 0, false), path);
    // expected.add(new Op(1002L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // // ///////////// redo ///////////////////////
    // map.get(key).redoWrite(OpType.Put, new RUD(1000L, 1, false), path);
    // execution.add(new Op(1000L, OpType.Put));
    // map.get(key).endRedoOp(OpType.Put, new RUD(1000L, 1, false), path);
    //
    // delayedOp = new Op(1001, OpType.Put);
    // this.start();
    //
    // map.get(key).redoWrite(OpType.Put, new RUD(1002L, 1, false), path);
    // execution.add(new Op(1002L, OpType.Put));
    // map.get(key).endRedoOp(OpType.Put, new RUD(1002L, 1, false), path);
    //
    // check();
    // }
    //

    //
    // @Test
    // public void ignore() {
    // map.trackReadAccess(key, new RUD(1000L, 0, false), path);
    // expected.add(new Op(1000L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1001L, 0, false), path);
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1002L, 0, false), path);
    // expected.add(new Op(1002L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackWriteAccess(key, OpType.Put, new RUD(1003L, 0, false), path);
    // expected.add(new Op(1003L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // // ///////////// redo ///////////////////////
    // map.get(key).ignore(new RUD(1001L, 1, false), path);
    //
    // map.get(key).redoRead(new RUD(1000L, 1, false), path);
    // execution.add(new Op(1000L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1000L, 1, false), path);
    //
    // map.get(key).redoRead(new RUD(1002L, 1, false), path);
    // execution.add(new Op(1002L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1002L, 1, false), path);
    //
    // // should not wait for 1001
    // map.get(key).redoWrite(OpType.Put, new RUD(1003L, 1, false), path);
    // execution.add(new Op(1003L, OpType.Put));
    // map.get(key).endRedoOp(OpType.Put, new RUD(1003L, 1, false), path);
    //
    // check();
    //
    // }
    //
    // @Test
    // public void ignoreWithWake() {
    // map.trackReadAccess(key, new RUD(1000L, 0, false), path);
    // expected.add(new Op(1000L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1001L, 0, false), path);
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1002L, 0, false), path);
    // expected.add(new Op(1002L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackWriteAccess(key, OpType.Put, new RUD(1003L, 0, false), path);
    // expected.add(new Op(1003L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // // ///////////// redo ///////////////////////
    // ignoreRud = new RUD(1001L, 1, false);
    // this.start();
    //
    // map.get(key).redoRead(new RUD(1000L, 1, false), path);
    // execution.add(new Op(1000L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1000L, 1, false), path);
    //
    // map.get(key).redoRead(new RUD(1002L, 1, false), path);
    // execution.add(new Op(1002L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1002L, 1, false), path);
    //
    // // should not wait for 1001
    // map.get(key).redoWrite(OpType.Put, new RUD(1003L, 1, false), path);
    // execution.add(new Op(1003L, OpType.Put));
    // map.get(key).endRedoOp(OpType.Put, new RUD(1003L, 1, false), path);
    //
    // check();
    //
    // }
    //
    // @Test
    // public void simulationRead() {
    // map.trackReadAccess(key, new RUD(1000L, 0, false), path);
    // expected.add(new Op(1000L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1001L, 0, false), path);
    // expected.add(new Op(1001L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackReadAccess(key, new RUD(1002L, 0, false), path);
    // expected.add(new Op(1002L, OpType.Get));
    // map.endReadAccess(key);
    //
    // map.trackWriteAccess(key, OpType.Put, new RUD(1003L, 0, false), path);
    // expected.add(new Op(1003L, OpType.Put));
    // map.endWriteAccess(key);
    //
    // // ///////////// redo ///////////////////////
    //
    // delayedOp = new Op(1003, OpType.Put);
    // this.start();
    //
    // map.get(key).redoRead(new RUD(1000L, 1, false), path);
    // execution.add(new Op(1000L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1000L, 1, false), path);
    //
    // map.get(key).redoRead(new RUD(1002L, 1, false), path);
    // execution.add(new Op(1002L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1002L, 1, false), path);
    //
    // try {
    // sleep(500);
    // } catch(InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // map.get(key).redoRead(new RUD(1001L, 1, false), path);
    // execution.add(new Op(1001L, OpType.Get));
    // map.get(key).endRedoOp(OpType.Get, new RUD(1001L, 1, false), path);
    //
    // // wait 1003 to exec
    // try {
    // sleep(500);
    // } catch(InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // check();
    // }

    @Test
    public void problemSimulation() {
        // [Get rid=1405007701228], [Put rid=1405007701228], [Get
        // rid=1405007701752], [Put rid=1405007701752]
        map.trackReadAccess(key, new RUD(1405007701228L, 0, false), path);
        expected.add(new Op(1405007701228L, OpType.Get));
        map.endReadAccess(key);

        map.trackWriteAccess(key, OpType.Put, new RUD(1405007701228L, 0, false), path);
        expected.add(new Op(1405007701228L, OpType.Put));
        map.endWriteAccess(key);

        map.trackReadAccess(key, new RUD(1405007701752L, 0, false), path);
        expected.add(new Op(1405007701752L, OpType.Get));
        map.endReadAccess(key);

        map.trackWriteAccess(key, OpType.Put, new RUD(1405007701752L, 0, false), path);
        expected.add(new Op(1405007701752L, OpType.Put));
        map.endWriteAccess(key);

        // ///////////// redo ///////////////////////

        map.get(key).redoRead(new RUD(1405007701228L, 1, false), path);
        execution.add(new Op(1405007701228L, OpType.Get));
        map.get(key).endRedoOp(OpType.Get, new RUD(1405007701228L, 1, false), path);

        map.get(key).redoWrite(OpType.Put, new RUD(1405007701228L, 1, false), path);
        execution.add(new Op(1405007701228L, OpType.Put));
        map.get(key).endRedoOp(OpType.Put, new RUD(1405007701228L, 1, false), path);

        map.get(key).redoRead(new RUD(1405007701752L, 1, false), path);
        execution.add(new Op(1405007701752L, OpType.Get));
        map.get(key).endRedoOp(OpType.Get, new RUD(1405007701752L, 1, false), path);

        map.get(key).redoWrite(OpType.Put, new RUD(1405007701752L, 1, false), path);
        execution.add(new Op(1405007701752L, OpType.Put));
        map.get(key).endRedoOp(OpType.Put, new RUD(1405007701752L, 1, false), path);

        check();
    }

    @Override
    public void run() {
        try {
            sleep(500);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        if(ignoreRud != null) {
            map.get(key).ignore(ignoreRud, path);
            return;

        }

        if(delayedOp.type == OpType.Get) {
            map.get(key).redoRead(new RUD(delayedOp.rid, 1, false), path);
            execution.add(new Op(delayedOp.rid, delayedOp.type));
            map.get(key).endRedoOp(delayedOp.type, new RUD(delayedOp.rid, 1, false), path);
        } else {
            map.get(key).redoWrite(delayedOp.type, new RUD(delayedOp.rid, 1, false), path);
            execution.add(new Op(delayedOp.rid, delayedOp.type));
            map.get(key).endRedoOp(delayedOp.type, new RUD(delayedOp.rid, 1, false), path);
        }
    }

    private void check() {
        try {
            for(int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).type, execution.get(i).type);
            }
        } catch(Exception e) {
            System.err.println(e);
        }
    }
}
