package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author darionascimento
 */
public class MultimapSync<K, V> {

    private static Object mutex = new Object();
    private HashMap<K, ArrayList<V>> map;
    final int EXPECTED_KEYS;
    final int EXPECTED_VALUES_PER_KEY;

    public MultimapSync(int expectedKeys, int expectedValuesPerKey) {
        EXPECTED_KEYS = expectedKeys;
        EXPECTED_VALUES_PER_KEY = expectedValuesPerKey;
        renew();
    }

    public MultimapSync() {
        EXPECTED_KEYS = 200;
        EXPECTED_VALUES_PER_KEY = 40;
        renew();
    }

    public void put(K key, V value) {
        synchronized(mutex) {
            ArrayList<V> l = map.get(key);
            if(l == null) {
                l = new ArrayList<V>(EXPECTED_VALUES_PER_KEY);
                map.put(key, l);
            }
            l.add(value);
        }
    }

    /**
     * Only one thread should renew the map
     * 
     * @return
     */
    public MultimapSyncView<K, V> renew() {
        HashMap<K, ArrayList<V>> newMap = new HashMap<K, ArrayList<V>>(EXPECTED_KEYS);
        HashMap<K, ArrayList<V>> old;
        synchronized(mutex) {
            old = this.map;
            this.map = newMap;
        }
        return new MultimapSyncView<K, V>(old);
    }
}
