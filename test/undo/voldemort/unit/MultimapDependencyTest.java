package voldemort.unit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultimap;

public class MultimapDependencyTest {

    ByteArray k1 = new ByteArray("key1".getBytes());

    /**
     * Test dependency inversion algorithm: a sequence of puts and gets to
     * extract the dependency and then update the dependency
     */
    @Test
    public void multimapDependencyTest() {
        OpMultimap map = new OpMultimap();
        map.put(k1, new Op(1, OpType.Put));
        map.put(k1, new Op(2, OpType.Get));
        map.put(k1, new Op(3, OpType.Get));
        map.put(k1, new Op(4, OpType.Delete));
        map.put(k1, new Op(5, OpType.Get));
        map.put(k1, new Op(6, OpType.Put));
        map.put(k1, new Op(7, OpType.Get));

        HashMultimap<Long, Long> d = HashMultimap.create();
        map.updateDependencies(d);
        System.out.println(d);

        assertTrue(d.get(7L).contains(6L) && d.get(7L).size() == 1);
        assertTrue(d.get(5L).contains(4L) && d.get(5L).size() == 1);
        assertTrue(d.get(3L).contains(1L) && d.get(3L).size() == 1);
        assertTrue(d.get(2L).contains(1L) && d.get(2L).size() == 1);

        // Add more and test again
        map.put(k1, new Op(8, OpType.Put));
        map.put(k1, new Op(9, OpType.Get));
        map.put(k1, new Op(10, OpType.Get));
        map.put(k1, new Op(11, OpType.Delete));
        map.put(k1, new Op(12, OpType.Get));
        map.put(k1, new Op(13, OpType.Put));
        map.put(k1, new Op(14, OpType.Get));

        map.updateDependencies(d);
        System.out.println(d);

        assertTrue(d.get(7L).contains(6L) && d.get(7L).size() == 1);
        assertTrue(d.get(5L).contains(4L) && d.get(5L).size() == 1);
        assertTrue(d.get(3L).contains(1L) && d.get(3L).size() == 1);
        assertTrue(d.get(2L).contains(1L) && d.get(2L).size() == 1);

        assertTrue(d.get(14L).contains(13L) && d.get(7L).size() == 1);
        assertTrue(d.get(12L).contains(11L) && d.get(5L).size() == 1);
        assertTrue(d.get(10L).contains(8L) && d.get(3L).size() == 1);
        assertTrue(d.get(9L).contains(8L) && d.get(2L).size() == 1);

    }
}
