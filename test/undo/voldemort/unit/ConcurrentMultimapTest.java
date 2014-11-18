package voldemort.unit;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.UpdateDependenciesMap;
import voldemort.utils.ByteArray;

/**
 * Test concurrent multimap test
 * 
 * @author darionascimento
 * 
 */
public class ConcurrentMultimapTest {

    private static UpdateDependenciesMap dependencyPerRid = new UpdateDependenciesMap();

    class TestThread extends Thread {

        KeyMap map;
        private final long nPuts;
        private final long nExtract;
        private final int keySpace;

        public TestThread(KeyMap map2, long nPuts, long nExtract, int keySpace) {
            super();
            this.map = map2;
            this.nPuts = nPuts;
            this.nExtract = nExtract;
            this.keySpace = keySpace;
        }

        @Override
        public void run() {
            Random r = new Random();
            for(long i = 0; i < nPuts; i++) {
                ByteArray key = new ByteArray(String.valueOf(r.nextInt(keySpace)).getBytes());
                Op op = new Op(r.nextLong(), OpType.Get);
                // TODO fix test map.put(key, op);
                if((i % nExtract) == 0) {
                    map.updateDependencies(dependencyPerRid);
                }
            }
            map.updateDependencies(dependencyPerRid);
        }

    }

    @Test
    public void testMap() throws InterruptedException {
        int N_THREADS = 10;
        long MILLION = 1000000L;
        long N_OPS = 40;
        long N_EXTRACT = 1;
        int KEY_SPACE = 200;
        KeyMap map = new KeyMap();
        Thread[] threads = new Thread[N_THREADS];
        for(int i = 0; i < N_THREADS; i++) {
            threads[i] = new TestThread(map, N_OPS, N_EXTRACT, KEY_SPACE);
        }

        // init keyspace with writes
        for(long i = 0; i <= KEY_SPACE; i++) {
            ByteArray key = new ByteArray(String.valueOf(i).getBytes());
            Op op = new Op(20, OpType.Put);
            // TODO fix test map.put(key, op);
        }

        for(int i = 0; i < N_THREADS; i++) {
            threads[i].start();
        }
        for(int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
        long c = dependencyPerRid.count();
        System.out.println(c);
        assertEquals(N_OPS * N_THREADS, c);
    }

    @Test
    public void putGet() {
        // TODO check
        DBProxy stub = new DBProxy();
        ByteArray k1 = new ByteArray("key1".getBytes());
        ByteArray k2 = k1.clone();
        stub.modifyKey(k1, (short) 0, 0L);
        KeyMap map = new KeyMap();
        // TODO fix test map.put(k1, new Op(20L, OpType.Delete));
        assertEquals(map.get(k1).size(), 1);

        // put 2
        // TODO fix test map.put(k1, new Op(21L, OpType.Get));
        assertEquals(map.get(k1).size(), 2);

        assertEquals(map.get(k2), null);
        stub.modifyKey(k2, (short) 0, 0L);
        assertEquals(map.get(k2).size(), 2);

        // TODO fix test map.put(k1, new Op(21L, OpType.Get));
        assertEquals(map.get(k2).size(), 3);

    }
}
