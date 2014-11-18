// package voldemort.replayAndSnapshot;
//
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.HashSet;
// import java.util.List;
//
// import junit.framework.Assert;
//
// import org.junit.Test;
//
// import voldemort.undoTracker.SRD;
// import voldemort.undoTracker.branching.BranchPath;
// import voldemort.undoTracker.map.KeyMapEntry;
// import voldemort.undoTracker.map.Op.OpType;
// import voldemort.undoTracker.map.VersionShuttle;
// import voldemort.utils.ByteArray;
//
// public class TestIgnore {
//
// KeyMapEntry entry;
// BranchPath path;
// List<String> order = Collections.synchronizedList(new ArrayList<String>());
//
// class DoOp extends Thread {
//
// @Override
// public void run() {
// order.add("Try read");
// entry.startNewOperation(OpType.Get, new SRD(22, 1, false), path);
// order.add("readed");
// }
// }
//
// /**
// * Verifies that the ignore system is working: the request 22 is locked
// * until the request 21 is unlocked
// *
// * @throws InterruptedException
// */
// @Test
// public void testIgnore() throws InterruptedException {
//
// ByteArray key = new ByteArray("key".getBytes());
// entry = new KeyMapEntry(key);
// VersionShuttle sts = new VersionShuttle(0, 0);
// HashSet<VersionShuttle> branch = new HashSet<VersionShuttle>();
// branch.add(sts);
// path = new BranchPath(sts, branch);
//
// entry.startNewOperation(OpType.GetVersion, new SRD(21, 1, false), path);
// entry.startNewOperation(OpType.Put, new SRD(21, 1, false), path);
// entry.startNewOperation(OpType.Get, new SRD(22, 1, false), path);
//
// new DoOp().start();
// Thread.sleep(2000);
// order.add("Try ignore");
// entry.ignore(new SRD(21, 1, false), path);
// order.add("Ignored");
// Thread.sleep(2000);
// Assert.assertTrue(order.contains("readed"));
// Assert.assertTrue(order.indexOf("Try ignore") < order.indexOf("readed"));
//
// }
// }
