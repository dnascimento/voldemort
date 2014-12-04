package voldemort.replayAndSnapshot;

import voldemort.undoTracker.map.Op;
import voldemort.utils.ByteArray;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class FakeDB {

    ListMultimap<ByteArray, Op> multimap = ArrayListMultimap.create();

    public void newOperation(ByteArray key, voldemort.undoTracker.map.Op op) {
        multimap.put(key.clone(), op);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(ByteArray key: multimap.keySet()) {
            sb.append(key + ": ");
            for(Op op: multimap.get(key)) {
                sb.append(op);
                sb.append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
