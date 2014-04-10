package voldemort.undoTracker;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

import voldemort.undoTracker.proto.ToManagerProto;
import voldemort.undoTracker.proto.ToManagerProto.TrackEntry;
import voldemort.undoTracker.proto.ToManagerProto.TrackMsg;

import com.google.common.collect.Multimap;

public class SendDependencies extends Thread {

    private Multimap<Long, Long> dependencies;
    private final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 9090);

    public SendDependencies(Multimap<Long, Long> dependencies) {
        this.dependencies = dependencies;
    }

    @Override
    public void run() {
        TrackMsg list = convertToList();
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
    private void send(TrackMsg list) throws IOException {
        if(list.getEntryCount() == 0) {
            return;
        }
        System.out.println("Sending dependencies to manager");
        Socket socket = new Socket();
        try {
            socket.connect(SERVER_ADDRESS);
            list.writeTo(socket.getOutputStream());
        } catch(ConnectException e) {
            System.out.println("Manager is off, the package is:");
            show(list);
        } finally {
            socket.close();
        }
    }

    private void show(TrackMsg list) {
        // TODO maybe improve
        System.out.println(list.toString());
    }

    /**
     * For each key, get the list and convert to tracklist
     * 
     * @return
     */
    private ToManagerProto.TrackMsg convertToList() {
        TrackMsg.Builder listBuilder = TrackMsg.newBuilder();
        for(Long key: dependencies.keySet()) {
            TrackEntry.Builder entry = TrackEntry.newBuilder();
            entry.setRid(key);
            entry.addAllDependency(dependencies.get(key));
            listBuilder.addEntry(entry.build());
        }
        return listBuilder.build();
    }

}
