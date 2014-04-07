package undo;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import voldemort.undoTracker.map.MultimapSync;
import voldemort.undoTracker.map.MultimapSyncView;

/**
 * Test concurrent multimap test
 * 
 * @author darionascimento
 * 
 */
public class ConcurrentMultimapTest {

    private static AtomicLong globalCount = new AtomicLong(0);

    class TestThread extends Thread {

        MultimapSync<String, Long> map;
        private final long nPuts;
        private final long nExtract;
        private final int keySpace;

        public TestThread(MultimapSync<String, Long> map2, long nPuts, long nExtract, int keySpace) {
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
                String key = String.valueOf(r.nextInt(keySpace));
                map.put(key, r.nextLong());
                if((i % nExtract) == 0) {
                    MultimapSyncView<String, Long> old = map.renew();
                    count(old);
                }
            }
            MultimapSyncView<String, Long> old = map.renew();
            count(old);
        }

        private void count(MultimapSyncView<String, Long> old) {
            long counter = 0L;
            for(String key: old.keySet()) {
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
        MultimapSync<String, Long> map = new MultimapSync<String, Long>(200, 20);
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
