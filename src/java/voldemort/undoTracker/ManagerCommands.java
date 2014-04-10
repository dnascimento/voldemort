package voldemort.undoTracker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.proto.FromManagerProto.Snapshot;

public class ManagerCommands extends Thread {

    private final static Logger log = LogManager.getLogger("ManagerCommands");
    private static ManagerCommands standalone;
    private final int DATABASE_TO_MANAGER_PORT = 9500;
    private ServerSocket serverSocket;
    private List<DBUndoStub> stubs = new ArrayList<DBUndoStub>();

    public ManagerCommands() throws IOException {
        serverSocket = new ServerSocket(DATABASE_TO_MANAGER_PORT);
    }

    @Override
    public void run() {
        while(true) {
            try {
                Socket s = serverSocket.accept();
                processRequest(s);
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processRequest(Socket s) throws IOException {
        Snapshot cmd = Snapshot.parseFrom(s.getInputStream());
        for(DBUndoStub stub: stubs) {
            stub.setNewSnapshotRid(cmd.getSeasonId());
        }
    }

    public static void register(DBUndoStub dbUndoStub) {
        log.info("Register new ManagerCommands");
        if(standalone == null) {
            try {
                standalone = new ManagerCommands();
            } catch(IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            standalone.run();
        }
        standalone.stubs.add(dbUndoStub);
    }
}
