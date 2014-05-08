/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import undo.proto.FromManagerProto;
import undo.proto.FromManagerProto.ToDataNode;
import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.NodeRegistryMsg;
import undo.proto.ToManagerProto.NodeRegistryMsg.NodeGroup;

public class ServiceDBNode extends Thread {

    private final Logger log = LogManager.getLogger(ServiceDBNode.class.getName());
    private ServerSocket serverSocket;
    private DBUndoStub stub;
    private boolean running = false;

    public ServiceDBNode(DBUndoStub stub) throws IOException {
        try {
            serverSocket = new ServerSocket(DBUndoStub.MY_PORT);
            this.stub = stub;
            running = true;
            System.out.println("DBNode service listening....");
            registryToManger();
        } catch(BindException e) {
            log.error(e);
        }
    }

    private void registryToManger() {
        Socket s = new Socket();
        try {
            s.connect(DBUndoStub.MANAGER_ADDRESS);
            NodeRegistryMsg c = ToManagerProto.NodeRegistryMsg.newBuilder()
                                                              .setHostname("localhost")
                                                              .setPort(DBUndoStub.MY_PORT)
                                                              .setGroup(NodeGroup.DB_NODE)
                                                              .build();
            ToManagerProto.MsgToManager.newBuilder()
                                       .setNodeRegistry(c)
                                       .build()
                                       .writeTo(s.getOutputStream());

            s.close();
        } catch(IOException e) {
            log.error("Manager not available");
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
        ToDataNode cmd = FromManagerProto.ToDataNode.parseFrom(s.getInputStream());
        if(cmd.hasSeasonId()) {
            stub.setNewCommitRid(cmd.getSeasonId());
        }
        if(cmd.hasResetDependencies()) {
            stub.resetDependencies();
        }
        if(cmd.hasUnlockNewBranch()) {
            stub.unlockRestrain((short) cmd.getUnlockNewBranch());
        }
    }
}
