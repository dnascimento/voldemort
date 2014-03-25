package voldemort.undoTracker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import voldemort.undoTracker.proto.OpProto;
import voldemort.undoTracker.proto.OpProto.TrackEntry;
import voldemort.utils.ByteArray;

public class SendOpTrack extends Thread {

    private static SendOpTrack singleton;

    private HashMap<Long, HashSet<Long>> dependencyPerRid;
    private ConcurrentHashMap<ByteArray, LinkedList<Op>> trackLocalReads;
    private ConcurrentHashMap<ByteArray, LinkedList<Op>> localReadArchive = new ConcurrentHashMap<ByteArray, LinkedList<Op>>();
    private final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 9090);
    private long REFRESH_PERIOD = 5000;

    public SendOpTrack(ConcurrentHashMap<ByteArray, LinkedList<Op>> trackLocalAccess) {
        super();
        this.trackLocalReads = trackLocalAccess;
    }

    @Override
    public void run() {
        while(true) {
            try {
                System.out.println("Extracting operations");
                extractOperations();
                sleep(REFRESH_PERIOD);
            } catch(IOException e) {
                e.printStackTrace();
                // TODO logger
            } catch(InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /**
     * Invert the index from per key to per RID. Keep the lastwrite as head of
     * list per key.
     * 
     * @throws IOException
     */
    private void extractOperations() throws IOException {
        dependencyPerRid = new HashMap<Long, HashSet<Long>>();
        for(ByteArray key: trackLocalReads.keySet()) {
            LinkedList<Op> opList = trackLocalReads.remove(key);
            assert (opList != null);
            Op lastWrite = invertDependency(opList);
            addToArchive(key, opList);
            // keep the last write on list
            opList = trackLocalReads.get(key);
            if(opList == null) {
                opList = new LinkedList<Op>();
                trackLocalReads.put(key, opList);
            }
            opList.addFirst(lastWrite);
        }
        OpProto.TrackList list = convertToList();
        send(list);
    }

    /**
     * Add the current list to archive
     * 
     * @param key
     * @param opList
     */
    private void addToArchive(ByteArray key, LinkedList<Op> opList) {
        LinkedList<Op> archiveList = localReadArchive.get(key);
        if(archiveList == null) {
            archiveList = new LinkedList<Op>();
            localReadArchive.put(key, archiveList);
        } else {
            // remove the last write kept from previous list
            opList.removeFirst();
        }
        archiveList.addAll(opList);
    }

    private OpProto.TrackList convertToList() {
        OpProto.TrackList.Builder listBuilder = OpProto.TrackList.newBuilder();
        for(Entry<Long, HashSet<Long>> ridPair: dependencyPerRid.entrySet()) {
            OpProto.TrackEntry.Builder entry = TrackEntry.newBuilder();
            entry.setRid(ridPair.getKey());
            entry.addAllDependencies(ridPair.getValue());
            listBuilder.addEntry(entry.build());
        }
        return listBuilder.build();
    }

    /**
     * Convert the hashmap to lists and send
     * 
     * @throws IOException
     */
    private void send(OpProto.TrackList list) throws IOException {
        if(list.getEntryCount() == 0) {
            return;
        }
        Socket socket = new Socket();
        socket.connect(SERVER_ADDRESS);
        list.writeTo(socket.getOutputStream());
        socket.close();
    }

    /**
     * Transforms the execution list of each key to the dependencies of each RID
     * 
     * @param opList
     */
    private Op invertDependency(LinkedList<Op> opList) {
        assert (opList.peekFirst().type == Op.OpType.Write);

        Iterator<Op> it = opList.iterator();
        Op lastWrite = it.next();

        while(it.hasNext()) {
            Op op = it.next();
            if(op.type == Op.OpType.Read) {
                HashSet<Long> dependencies = dependencyPerRid.get(op.rid);
                if(dependencies == null) {
                    dependencies = new HashSet<Long>();
                    dependencyPerRid.put(op.rid, dependencies);
                }
                dependencies.add(lastWrite.rid);
            } else {
                lastWrite = op;
            }
        }
        return lastWrite;
    }

    public synchronized static void init(ConcurrentHashMap<ByteArray, LinkedList<Op>> trackLocalAccess) {
        if(singleton == null) {
            singleton = new SendOpTrack(trackLocalAccess);
            singleton.start();
        }
    }
}
