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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import undo.proto.FromManagerProto;
import undo.proto.FromManagerProto.ToDataNode;
import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.EntryAccessList;
import undo.proto.ToManagerProto.MsgToManager;
import undo.proto.ToManagerProto.NodeRegistryMsg;
import undo.proto.ToManagerProto.NodeRegistryMsg.NodeGroup;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.StsBranchPair;

import com.google.protobuf.ByteString;

public class ServiceDBNode extends Thread {

    private final Logger log = Logger.getLogger(ServiceDBNode.class.getName());
    private ServerSocket serverSocket;
    private DBProxy stub;
    private boolean running = false;

    public ServiceDBNode(DBProxy stub) throws IOException {
        try {
            serverSocket = new ServerSocket(DBProxy.MY_PORT);
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
            s.connect(DBProxy.MANAGER_ADDRESS);
            NodeRegistryMsg c = ToManagerProto.NodeRegistryMsg.newBuilder()
                                                              .setHostname("localhost")
                                                              .setPort(DBProxy.MY_PORT)
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
        // Get the access list of a specific key for selective replay
        if(cmd.hasEntryAccessesMsg()) {
            HashMap<ByteString, ArrayList<Op>> result = stub.getAccessList(cmd.getEntryAccessesMsg()
                                                                              .getKeysList(),
                                                                           cmd.getEntryAccessesMsg()
                                                                              .getBaseRid());
            MsgToManager.Builder b = ToManagerProto.MsgToManager.newBuilder();
            for(Entry<ByteString, ArrayList<Op>> entry: result.entrySet()) {
                EntryAccessList.Builder entryBuilder = EntryAccessList.newBuilder()
                                                                      .setKey(entry.getKey());
                for(Op o: entry.getValue()) {
                    entryBuilder.addRid(o.rid);
                }
                b.addEntryAccessList(entryBuilder);
            }
            b.build().writeDelimitedTo(s.getOutputStream());
            s.getOutputStream().flush();
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
