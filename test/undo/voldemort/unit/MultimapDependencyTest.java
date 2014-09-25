package voldemort.unit;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.UpdateDependenciesMap;
import voldemort.undoTracker.map.UpdateDependenciesMap.WrapperLong;
import voldemort.utils.ByteArray;

public class MultimapDependencyTest {

    ByteArray k1 = new ByteArray("key1".getBytes());

    /**
     * Test dependency inversion algorithm: a sequence of puts and gets to
     * extract the dependency and then update the dependency
     */
    @Test
    public void multimapDependencyTest() {
        OpMultimap map = new OpMultimap();
        // TODO fix test map.put(k1, new Op(1, OpType.Put));
        // TODO fix test map.put(k1, new Op(2, OpType.Get));
        // TODO fix test map.put(k1, new Op(3, OpType.Get));
        // TODO fix test map.put(k1, new Op(4, OpType.Delete));
        // TODO fix test map.put(k1, new Op(5, OpType.Get));
        // TODO fix test map.put(k1, new Op(6, OpType.Put));
        // TODO fix test map.put(k1, new Op(7, OpType.Get));

        UpdateDependenciesMap d = new UpdateDependenciesMap();
        map.updateDependencies(d);
        HashMap<Long, WrapperLong> table = d.createDependencyMap();
        System.out.println(table);

        assertTrue(contains(table.get(7L), 6L) && checkLength(table.get(7L), 1));
        assertTrue(contains(table.get(5L), 4L) && checkLength(table.get(5L), 1));
        assertTrue(contains(table.get(3L), 1L) && checkLength(table.get(3L), 1));
        assertTrue(contains(table.get(2L), 1L) && checkLength(table.get(2L), 1));

        // Add more and test again
        // TODO fix test map.put(k1, new Op(8, OpType.Put));
        // TODO fix test map.put(k1, new Op(9, OpType.Get));
        // TODO fix test map.put(k1, new Op(10, OpType.Get));
        // TODO fix test map.put(k1, new Op(11, OpType.Delete));
        // TODO fix test map.put(k1, new Op(12, OpType.Get));
        // TODO fix test map.put(k1, new Op(13, OpType.Put));
        // TODO fix test map.put(k1, new Op(14, OpType.Get));

        d = new UpdateDependenciesMap();
        map.updateDependencies(d);
        table = d.createDependencyMap();
        System.out.println(table);

        assertTrue(contains(table.get(14L), 13L) && checkLength(table.get(14L), 1));
        assertTrue(contains(table.get(12L), 11L) && checkLength(table.get(12L), 1));
        assertTrue(contains(table.get(10L), 8L) && checkLength(table.get(10L), 1));
        assertTrue(contains(table.get(9L), 8L) && checkLength(table.get(9L), 1));
    }

    private boolean contains(WrapperLong wrapper, long goal) {
        long[] array = wrapper.array;
        for(int i = 0; i < array.length; i++) {
            if(array[i] == goal)
                return true;
        }
        return false;
    }

    private boolean checkLength(WrapperLong wrapper, int length) {
        int count = 0;
        while(wrapper.hasNext()) {
            wrapper.next();
            count++;
        }
        return count == length;

    }
}
