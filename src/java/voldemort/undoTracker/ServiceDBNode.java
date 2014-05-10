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
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import undo.proto.FromManagerProto;
import undo.proto.FromManagerProto.ToDataNode;
import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.NodeRegistryMsg;
import undo.proto.ToManagerProto.NodeRegistryMsg.NodeGroup;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.StsBranchPair;

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
            log.info("DBNode service listening....");
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
                                       .writeDelimitedTo(s.getOutputStream());

            s.close();
        } catch(IOException e) {
            log.error("Manager not available");
        }
    }

    @Override
    public void run() {
        while(running) {
            Socket s = null;
            try {
                s = serverSocket.accept();
                processRequest(s);
            } catch(IOException e) {
                log.error(e);
            }
            if(s != null) {
                try {
                    s.close();
                } catch(IOException e) {
                    log.error(e);
                }
            }
        }
    }

    private void processRequest(Socket s) throws IOException {
        ToDataNode cmd = FromManagerProto.ToDataNode.parseDelimitedFrom(s.getInputStream());
        if(cmd.hasNewCommit()) {
            stub.scheduleNewCommit(cmd.getNewCommit());
            ToManagerProto.AckMsg.newBuilder().build().writeDelimitedTo(s.getOutputStream());
            s.getOutputStream().flush();
        }
        if(cmd.hasResetDependencies()) {
            stub.resetDependencies();
        }
        if(cmd.hasRedoOver()) {
            stub.redoOver();
        }
        if(cmd.getPathBranchCount() != 0 && cmd.getPathCommitCount() != 0) {
            // Retrieve 2 sorted list from most recent to oldest
            Iterator<Long> itCommits = cmd.getPathCommitList().iterator();
            Iterator<Integer> itBranches = cmd.getPathBranchList().iterator();
            HashSet<StsBranchPair> path = new HashSet<StsBranchPair>();
            StsBranchPair current = new StsBranchPair(cmd.getPathCommit(0), cmd.getPathBranch(0));
            while(itCommits.hasNext()) {
                path.add(new StsBranchPair(itCommits.next(), itBranches.next()));
            }
            BranchPath branchPath = new BranchPath(current, path);
            stub.newRedo(branchPath);
            ToManagerProto.AckMsg.newBuilder().build().writeDelimitedTo(s.getOutputStream());
            s.getOutputStream().flush();
        }
        s.close();
    }
}
