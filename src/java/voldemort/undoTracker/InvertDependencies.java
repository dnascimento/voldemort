package voldemort.undoTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapView;
import voldemort.utils.ByteArray;

import com.google.common.collect.LinkedListMultimap;

/**
 * Just one thread to collect the results, store in memory and send.
 * Its a parallel process.
 * 
 * @author darionascimento
 * 
 */
public class InvertDependencies extends Thread {

    private OpMultimap archive;
    private OpMultimap currentRegistry;
    private long REFRESH_PERIOD = 5000;

    public InvertDependencies(OpMultimap trackLocalAccess, OpMultimap archive) {
        this.currentRegistry = trackLocalAccess;
        this.archive = archive;
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
        OpMultimapView view = currentRegistry.renew();
        if(view.isEmpty())
            return;

        LinkedListMultimap<Long, Long> dependencyPerRid = LinkedListMultimap.create();
        for(ByteArray key: view.keySet()) {
            invertDependency(key, view.get(key), dependencyPerRid);
            // Add the current list to archive
            archive.putAll(key, view.get(key));
        }
        SendDependencies d = new SendDependencies(dependencyPerRid);
        d.start();

    }

    /**
     * Transforms the execution list of each key to the dependencies of
     * each RID
     * 
     * @param key
     * 
     * @param opList
     * @param dependencyPerRid
     */
    private Op invertDependency(ByteArray key,
                                Collection<Op> opList,
                                LinkedListMultimap<Long, Long> dependencyPerRid) {

        Iterator<Op> it = opList.iterator();
        Op lastWrite = getLastWrite(key);
        if(lastWrite == null) {
            lastWrite = it.next();
        }
        System.out.println("LAST WRITE:" + lastWrite.type);
        assert (lastWrite.type != OpType.Get);
        while(it.hasNext()) {
            Op op = it.next();
            if(op.type == Op.OpType.Get) {
                dependencyPerRid.put(op.rid, lastWrite.rid);
            } else {
                lastWrite = op;
            }
        }
        return lastWrite;
    }

    private Op getLastWrite(ByteArray key) {
        return archive.getLastWrite(key);
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
