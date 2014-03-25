package voldemort;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import voldemort.undoTracker.Op;
import voldemort.undoTracker.SendOpTrack;
import voldemort.utils.ByteArray;

public class UndoTest {

    @Test
    public void test() {
        // TODO fix
        ConcurrentHashMap<ByteArray, LinkedList<Op>> map = new ConcurrentHashMap<ByteArray, LinkedList<Op>>();
        LinkedList<Op> list = new LinkedList<Op>();
        list.addLast(new Op(11, Op.OpType.Write));
        list.addLast(new Op(12, Op.OpType.Read));
        map.put(new ByteArray((byte) 1), list);
        new SendOpTrack(map).start();
    }

}
