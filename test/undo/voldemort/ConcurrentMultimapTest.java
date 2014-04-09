package undo;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapView;
import voldemort.utils.ByteArray;

/**
 * Test concurrent multimap test
 * 
 * @author darionascimento
 * 
 */
public class ConcurrentMultimapTest {

    private static AtomicLong globalCount = new AtomicLong(0);

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
                    OpMultimapView old = map.renew();
                    count(old);
                }
            }
            OpMultimapView old = map.renew();
            count(old);
        }

        private void count(OpMultimapView old) {
            long counter = 0L;
            for(ByteArray key: old.keySet()) {
                counter += old.get(key).size();
            }
            ConcurrentMultimapTest.globalCount.addAndGet(counter);
        }
    }

    @Test
    public void testMap() throws InterruptedException {
        int N_THREADS = 40;
        long MILLION = 1000000L;
        long N_PUTS = 1 * MILLION;
        long N_EXTRACT = 200;
        int KEY_SPACE = 20;
        OpMultimap map = new OpMultimap();
        Thread[] threads = new Thread[N_THREADS];
        for(int i = 0; i < N_THREADS; i++) {
            threads[i] = new TestThread(map, N_PUTS, N_EXTRACT, KEY_SPACE);
        }

        for(int i = 0; i < N_THREADS; i++) {
            threads[i].start();
        }
        for(int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
        System.out.println(globalCount.get());
    }
}
