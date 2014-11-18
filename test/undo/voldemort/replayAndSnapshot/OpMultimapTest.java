// package voldemort.replayAndSnapshot;
//
// import static org.junit.Assert.assertEquals;
//
// import java.util.LinkedList;
//
// import org.junit.Before;
// import org.junit.Test;
//
// import voldemort.undoTracker.SRD;
// import voldemort.undoTracker.branching.BranchPath;
// import voldemort.undoTracker.map.KeyMap;
// import voldemort.undoTracker.map.Op;
// import voldemort.undoTracker.map.Op.OpType;
// import voldemort.undoTracker.map.VersionShuttle;
// import voldemort.utils.ByteArray;
//
// public class OpMultimapTest extends Thread {
//
// ByteArray key = new ByteArray("batatas".getBytes());
// VersionShuttle current = new VersionShuttle(00L, 00);
// BranchPath path = new BranchPath(current, current);
// private KeyMap map;
// private SRD ignoreRud;
//
// LinkedList<Op> expected;
// LinkedList<Op> execution;
// Op delayedOp;
//
// @Before
// public void prepare() {
// expected = new LinkedList<Op>();
// execution = new LinkedList<Op>();
// map = new KeyMap();
// ignoreRud = null;
// }
//
// @Test
// public void simulation() {
// // map.trackWriteAccess(key, OpType.Put, new SRD(1000L, 0, false),
// // path);
// // expected.add(new Op(1000L, OpType.Put));
// // map.endWriteAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1001L, 0, false),
// // path);
// // expected.add(new Op(1001L, OpType.Put));
// // map.endWriteAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1002L, 0, false),
// // path);
// // expected.add(new Op(1002L, OpType.Put));
// // map.endWriteAccess(key);
//
// // ///////////// replay ///////////////////////
// replayOp(OpType.Put, new SRD(1000L, 1, false));
// delayedOp = new Op(1001, OpType.Put);
// this.start();
// replayOp(OpType.Put, new SRD(1002L, 1, false));
// check();
// }
//
// private void replayOp(OpType type, SRD srd) {
// Op op = new Op(srd.rid, type);
// map.get(key).startReplayOperation(op, srd, path);
// execution.add(op);
// map.get(key).endReplayOperation(op.toType(), srd, path);
// }
//
// @Test
// public void ignore() {
// // map.trackReadAccess(key, new SRD(1000L, 0, false), path);
// // expected.add(new Op(1000L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1001L, 0, false), path);
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1002L, 0, false), path);
// // expected.add(new Op(1002L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1003L, 0, false),
// // path);
// // expected.add(new Op(1003L, OpType.Put));
// // map.endWriteAccess(key);
//
// // ///////////// replay ///////////////////////
// map.get(key).ignore(new SRD(1001L, 1, false), path);
//
// replayOp(OpType.Get, new SRD(1000L, 1, false));
// replayOp(OpType.Get, new SRD(1002L, 1, false));
// // should not wait for 1001
// replayOp(OpType.Put, new SRD(1003L, 1, false));
// check();
//
// }
//
// @Test
// public void ignoreWithWake() {
// // map.trackReadAccess(key, new SRD(1000L, 0, false), path);
// // expected.add(new Op(1000L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1001L, 0, false), path);
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1002L, 0, false), path);
// // expected.add(new Op(1002L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1003L, 0, false),
// // path);
// // expected.add(new Op(1003L, OpType.Put));
// // map.endWriteAccess(key);
//
// // ///////////// replay ///////////////////////
// ignoreRud = new SRD(1001L, 1, false);
// this.start();
//
// replayOp(OpType.Get, new SRD(1000L, 1, false));
// replayOp(OpType.Get, new SRD(1002L, 1, false));
// // should not wait for 1001
// replayOp(OpType.Put, new SRD(1003L, 1, false));
// check();
//
// }
//
// @Test
// public void simulationRead() {
// // map.trackReadAccess(key, new SRD(1000L, 0, false), path);
// // expected.add(new Op(1000L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1001L, 0, false), path);
// // expected.add(new Op(1001L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1002L, 0, false), path);
// // expected.add(new Op(1002L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1003L, 0, false),
// // path);
// // expected.add(new Op(1003L, OpType.Put));
// // map.endWriteAccess(key);
//
// // ///////////// replay ///////////////////////
//
// delayedOp = new Op(1003, OpType.Put);
// this.start();
//
// replayOp(OpType.Get, new SRD(1000L, 1, false));
// replayOp(OpType.Get, new SRD(1002L, 1, false));
//
// try {
// sleep(500);
// } catch(InterruptedException e) {
// e.printStackTrace();
// }
// replayOp(OpType.Get, new SRD(1001L, 1, false));
// // wait 1003 to exec
// try {
// sleep(500);
// } catch(InterruptedException e) {
// e.printStackTrace();
// }
// check();
// }
//
// @Test
// public void problemSimulation() {
// // [Get rid=1405007701228], [Put rid=1405007701228], [Get
// // rid=1405007701752], [Put rid=1405007701752]
// // map.trackReadAccess(key, new SRD(1405007701228L, 0, false), path);
// // expected.add(new Op(1405007701228L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1405007701228L, 0,
// // false), path);
// // expected.add(new Op(1405007701228L, OpType.Put));
// // map.endWriteAccess(key);
// //
// // map.trackReadAccess(key, new SRD(1405007701752L, 0, false), path);
// // expected.add(new Op(1405007701752L, OpType.Get));
// // map.endReadAccess(key);
// //
// // map.trackWriteAccess(key, OpType.Put, new SRD(1405007701752L, 0,
// // false), path);
// // expected.add(new Op(1405007701752L, OpType.Put));
// // map.endWriteAccess(key);
//
// // ///////////// replay ///////////////////////
// replayOp(OpType.Get, new SRD(1405007701228L, 1, false));
// replayOp(OpType.Put, new SRD(1405007701228L, 1, false));
// replayOp(OpType.Get, new SRD(1405007701752L, 1, false));
// replayOp(OpType.Put, new SRD(1405007701752L, 1, false));
//
// check();
// }
//
// @Override
// public void run() {
// try {
// sleep(500);
// } catch(InterruptedException e) {
// e.printStackTrace();
// }
// if(ignoreRud != null) {
// map.get(key).ignore(ignoreRud, path);
// return;
//
// }
//
// replayOp(delayedOp.toType(), new SRD(delayedOp.rid, 1, false));
// }
//
// private void check() {
// try {
// for(int i = 0; i < expected.size(); i++) {
// assertEquals(expected.get(i).type, execution.get(i).type);
// }
// } catch(Exception e) {
// System.err.println(e);
// }
// }
// }
