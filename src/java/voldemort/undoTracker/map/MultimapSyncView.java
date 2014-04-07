package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Provides a view of ConcurrentMultimap.
 * This view is not synchronized
 * 
 * @author darionascimento
 * @param <K>
 * @param <V>
 */
public class MultimapSyncView<K, V> {

    private final HashMap<K, ArrayList<V>> map;

    public MultimapSyncView(HashMap<K, ArrayList<V>> old) {
        this.map = old;
    }

    /**
     * Return a key iterator.
     * 
     * @return
     */
    public Set<K> keySet() {
        return map.keySet();
    }

    public List<V> get(K key) {
        return map.get(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

}
