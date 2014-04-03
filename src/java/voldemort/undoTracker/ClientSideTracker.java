package voldemort.undoTracker;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class ClientSideTracker {

    private Multimap<Long, Long> dependencyPerRid;

    public ClientSideTracker() {
        super();
        LinkedListMultimap<Long, Long> map = LinkedListMultimap.create();
        this.dependencyPerRid = Multimaps.synchronizedListMultimap(map);
    }

    public void trackGet(long rid, long dependentRid) {
        dependencyPerRid.put(rid, dependentRid);
    }
}
