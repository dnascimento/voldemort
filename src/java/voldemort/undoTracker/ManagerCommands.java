package voldemort.undoTracker;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.proto.FromManagerProto.Snapshot;

public class ManagerCommands extends Thread {

    private final Logger log = LogManager.getLogger("ManagerCommands");
    private final int DATABASE_TO_MANAGER_PORT = 9600;
    private ServerSocket serverSocket;
    private DBUndoStub stub;
    private boolean running = false;

    public ManagerCommands(DBUndoStub stub) throws IOException {
        try {
            serverSocket = new ServerSocket(DATABASE_TO_MANAGER_PORT);
            this.stub = stub;
            running = true;
        } catch(BindException e) {

        }
    }

    @Override
    public void run() {
        while(running) {
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
        stub.setNewSnapshotRid(cmd.getSeasonId());
    }
}
