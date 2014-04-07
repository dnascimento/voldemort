package voldemort.undoTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import voldemort.undoTracker.Op.OpType;
import voldemort.undoTracker.map.MultimapSync;
import voldemort.undoTracker.map.MultimapSyncView;
import voldemort.utils.ByteArray;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;

/**
 * Just one thread to collect the results, store in memory and send.
 * Its a parallel process.
 * 
 * @author darionascimento
 * 
 */
public class InvertDependenciesAndSendThread extends Thread {

    private ArrayListMultimap<ByteArray, Op> archive = ArrayListMultimap.create();
    private MultimapSync<ByteArray, Op> currentRegistry;
    private long REFRESH_PERIOD = 5000;

    public InvertDependenciesAndSendThread(MultimapSync<ByteArray, Op> trackLocalAccess) {
        this.currentRegistry = trackLocalAccess;
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
        MultimapSyncView<ByteArray, Op> view = currentRegistry.renew();
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
        assert (lastWrite.type != OpType.Read);
        while(it.hasNext()) {
            Op op = it.next();
            if(op.type == Op.OpType.Read) {
                dependencyPerRid.put(op.rid, lastWrite.rid);
            } else {
                lastWrite = op;
            }
        }
        return lastWrite;
    }

    private Op getLastWrite(ByteArray key) {
        List<Op> array = archive.get(key);
        for(int i = array.size() - 1; i >= 0; i--) {
            if(array.get(i).type != OpType.Read) {
                return array.get(i);
            }
        }
        return null;
    }
}
