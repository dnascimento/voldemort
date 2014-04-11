package voldemort.undoTracker;

import java.io.IOException;
import java.util.List;

import voldemort.undoTracker.map.OpMultimap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;

/**
 * Just one thread to collect the results, store in memory and send.
 * Its a parallel process.
 * 
 * @author darionascimento
 * 
 */
public class InvertDependencies extends Thread {

    private OpMultimap trackLocalAccess;
    private long REFRESH_PERIOD = 5000;

    public InvertDependencies(OpMultimap trackLocalAccess) {
        this.trackLocalAccess = trackLocalAccess;
    }

    @Override
    public void run() {
        while(true) {
            try {
                extractOperations();
                sleep(REFRESH_PERIOD);
            } catch(IOException e) {
                e.printStackTrace();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Invert the index from per key to per RID. Keep the last write as head of
     * list per key.
     * 
     * @throws IOException
     */
    private void extractOperations() throws IOException {

        HashMultimap<Long, Long> dependencyPerRid = HashMultimap.create();
        trackLocalAccess.updateDependencies(dependencyPerRid);

        SendDependencies d = new SendDependencies(dependencyPerRid);
        d.start();

    }

    @SuppressWarnings("unused")
    private void show(LinkedListMultimap<Long, Long> map) {
        System.out.println("---- New Dependencies ----");
        for(Long rid: map.keySet()) {
            List<Long> deps = map.get(rid);
            System.out.print("Rid: ");
            System.out.print(rid);
            for(Long dep: deps) {
                System.out.print(dep);
                System.out.print(" ,");
            }
            System.out.print("\n");
        }
    }
}
