package voldemort.undoTracker;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

import voldemort.undoTracker.proto.OpProto;
import voldemort.undoTracker.proto.OpProto.TrackEntry;

import com.google.common.collect.Multimap;

public class SendDependencies extends Thread {

    private Multimap<Long, Long> dependencies;
    private final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 9090);

    public SendDependencies(Multimap<Long, Long> dependencies) {
        this.dependencies = dependencies;
    }

    @Override
    public void run() {
        OpProto.TrackList list = convertToList();
        try {
            send(list);
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
        System.out.println("Sending dependencies to manager");
        Socket socket = new Socket();
        try {
            socket.connect(SERVER_ADDRESS);
            list.writeTo(socket.getOutputStream());
        } catch(ConnectException e) {
            System.out.println("Manager is off");
        } finally {
            socket.close();
        }
    }

    /**
     * For each key, get the list and convert to tracklist
     * 
     * @return
     */
    private OpProto.TrackList convertToList() {
        OpProto.TrackList.Builder listBuilder = OpProto.TrackList.newBuilder();
        for(Long key: dependencies.keySet()) {
            OpProto.TrackEntry.Builder entry = TrackEntry.newBuilder();
            entry.setRid(key);
            entry.addAllDependencies(dependencies.get(key));
            listBuilder.addEntry(entry.build());
        }
        return listBuilder.build();
    }

}
