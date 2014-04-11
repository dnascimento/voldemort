package voldemort.unit;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultimap;

/**
 * Test concurrent multimap test
 * 
 * @author darionascimento
 * 
 */
public class ConcurrentMultimapTest {

    private static HashMultimap<Long, Long> dependencyPerRid = HashMultimap.create();

    class TestThread extends Thread {

        OpMultimap map;
        private final long nPuts;
        private final long nExtract;
        private final int keySpace;

        public TestThread(OpMultimap map2, long nPuts, long nExtract, int keySpace) {
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
                map.put(key, op);
                if((i % nExtract) == 0) {
                    map.updateDependencies(dependencyPerRid);
                }
            }
            map.updateDependencies(dependencyPerRid);
        }

    }

    @Test
    public void testMap() throws InterruptedException {
        int N_THREADS = 100;
        long MILLION = 1000000L;
        long N_OPS = 400;
        long N_EXTRACT = 1;
        int KEY_SPACE = 200;
        OpMultimap map = new OpMultimap();
        Thread[] threads = new Thread[N_THREADS];
        for(int i = 0; i < N_THREADS; i++) {
            threads[i] = new TestThread(map, N_OPS, N_EXTRACT, KEY_SPACE);
        }

        // init keyspace with writes
        for(long i = 0; i <= KEY_SPACE; i++) {
            ByteArray key = new ByteArray(String.valueOf(i).getBytes());
            Op op = new Op(20, OpType.Put);
            map.put(key, op);
        }

        for(int i = 0; i < N_THREADS; i++) {
            threads[i].start();
        }
        for(int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
        long c = count();
        System.out.println(c);
        assertEquals(N_OPS * N_THREADS, c);
    }

    private long count() {
        long counter = 0L;
        for(Long key: dependencyPerRid.keySet()) {
            counter += dependencyPerRid.get(key).size();
        }
        return counter;
    }

    @Test
    public void putGet() {
        ByteArray k1 = new ByteArray("key1".getBytes());
        ByteArray k2 = k1.clone();
        DBUndoStub.modifyKey(k1, (short) 0, 0L);
        OpMultimap map = new OpMultimap();
        map.put(k1, new Op(20L, OpType.Delete));
        assertEquals(map.get(k1).size(), 1);

        // put 2
        map.put(k1, new Op(21L, OpType.Get));
        assertEquals(map.get(k1).size(), 2);

        assertEquals(map.get(k2), null);
        DBUndoStub.modifyKey(k2, (short) 0, 0L);
        assertEquals(map.get(k2).size(), 2);

        map.put(k1, new Op(21L, OpType.Get));
        assertEquals(map.get(k2).size(), 3);

    }
}
