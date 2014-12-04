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
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import pt.inesc.undo.proto.FromManagerProto;
import pt.inesc.undo.proto.FromManagerProto.ToDataNode;
import pt.inesc.undo.proto.ToManagerProto;
import pt.inesc.undo.proto.ToManagerProto.MsgToManager;
import pt.inesc.undo.proto.ToManagerProto.MsgToManager.EntryAccessList;
import pt.inesc.undo.proto.ToManagerProto.MsgToManager.NodeRegistryMsg;
import pt.inesc.undo.proto.ToManagerProto.MsgToManager.NodeRegistryMsg.NodeGroup;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;

import com.google.protobuf.ByteString;

public class ServiceDBNode extends Thread {

    private final Logger log = Logger.getLogger(ServiceDBNode.class.getName());
    private ServerSocket serverSocket;
    private DBProxy stub;
    private boolean running = false;

    public ServiceDBNode(DBProxy stub) throws IOException {
        try {
            serverSocket = new ServerSocket(DBProxy.MY_ADDRESS.getPort());
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
            NodeRegistryMsg c = ToManagerProto.MsgToManager.NodeRegistryMsg.newBuilder()
                                                                           .setHostname(DBProxy.MY_ADDRESS.getHostName())
                                                                           .setPort(DBProxy.MY_ADDRESS.getPort())
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
        if(cmd.hasNewSnapshot()) {
            stub.scheduleNewSnapshot(cmd.getNewSnapshot());
            ToManagerProto.MsgToManager.AckMsg.newBuilder()
                                              .build()
                                              .writeDelimitedTo(s.getOutputStream());
            s.getOutputStream().flush();
        }
        if(cmd.hasShowMap()) {
            String debug = stub.keyMap.debugExecutionList();
            System.out.println(debug);
        }

        if(cmd.hasResetDependencies()) {
            stub.resetDependencies();
        }
        if(cmd.hasReplayOver()) {
            stub.replayOver();
        }

        if(cmd.hasShowStats()) {
            stub.measureMemoryFootPrint();
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

        if(cmd.hasBranchPath()) {
            // BranchPath of the branch in which the requests will be replated
            ToDataNode.BranchPath branchPathMsg = cmd.getBranchPath();

            BranchPath branchPath = new BranchPath((short) branchPathMsg.getBranch(),
                                                   branchPathMsg.getLatestVersion(),
                                                   branchPathMsg.getVersionsList());

            stub.newReplay(branchPath);
            ToManagerProto.MsgToManager.AckMsg.newBuilder()
                                              .build()
                                              .writeDelimitedTo(s.getOutputStream());
            s.getOutputStream().flush();
        }
        s.close();
    }
}
