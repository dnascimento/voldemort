/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.apache.log4j.Logger;

import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.MsgToManager;
import undo.proto.ToManagerProto.TrackEntry;
import undo.proto.ToManagerProto.TrackMsg;

import com.google.common.collect.Multimap;

public class SendDependencies extends Thread {

    private final Logger log = Logger.getLogger(SendDependencies.class.getName());
    private Multimap<Long, Long> dependencies;

    public SendDependencies(Multimap<Long, Long> dependencies) {
        this.dependencies = dependencies;
    }

    @Override
    public void run() {
        TrackMsg list = convertToList();
        try {
            send(list);
        } catch(IOException e) {
            log.error(e);
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
        log.info("Sending dependencies to manager");
        Socket socket = new Socket();
        try {
            socket.connect(DBProxy.MANAGER_ADDRESS);
            MsgToManager msg = ToManagerProto.MsgToManager.newBuilder().setTrackMsg(list).build();
            msg.writeDelimitedTo(socket.getOutputStream());
        } catch(ConnectException e) {
            log.error("Manager is off, the package is:");
            show(list);
        } finally {
            socket.close();
        }
    }

    private void show(TrackMsg list) {
        log.info(list.toString());
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
