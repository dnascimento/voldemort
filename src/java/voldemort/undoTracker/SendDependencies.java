/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.MsgToManager;
import undo.proto.ToManagerProto.TrackMsg;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.UpdateDependenciesMap;

import com.google.common.collect.LinkedListMultimap;

public class SendDependencies extends Thread {

    private final Logger log = Logger.getLogger(SendDependencies.class.getName());

    private OpMultimap trackLocalAccess;
    private long REFRESH_PERIOD = 1000000000;
    Socket socket = new Socket();

    public SendDependencies(OpMultimap trackLocalAccess) {
        log.setLevel(Level.ALL);
        this.trackLocalAccess = trackLocalAccess;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName("SendDependencies: " + threadName);

        while(true) {
            try {
                sleep(REFRESH_PERIOD);
                extractOperations();
            } catch(IOException e) {
                log.error("Extracting dependencies error", e);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Invert the index from per key to per RID. Keep the last write as head of
     * list per key.
     * 
     * @throws IOException
     */
    private void extractOperations() throws IOException {
        UpdateDependenciesMap dependenciesPerRid = new UpdateDependenciesMap();
        boolean newDeps = trackLocalAccess.updateDependencies(dependenciesPerRid);
        if(newDeps) {
            TrackMsg list = dependenciesPerRid.convertToList();
            if(list != null) {
                send(list);
            }
        }
    }

    /**
     * Convert the hashmap to lists and send
     * 
     * @throws IOException
     */
    private void send(TrackMsg list) {
        if(list.getEntryCount() == 0) {
            return;
        }
        log.info("Sending dependencies to manager");
        MsgToManager msg = ToManagerProto.MsgToManager.newBuilder().setTrackMsg(list).build();
        if(socket == null) {
            return;
        }

        socket = new Socket();
        try {
            socket.connect(DBProxy.MANAGER_ADDRESS);
            msg.writeDelimitedTo(socket.getOutputStream());
            socket.close();
        } catch(IOException e) {
            log.error(e);
        }
    }

    @SuppressWarnings("unused")
    private void show(LinkedListMultimap<Long, Long> map) {
        log.info("---- New Dependencies ----");
        for(long rid: map.keySet()) {
            List<Long> deps = map.get(rid);
            System.out.print("Rid: ");
            System.out.print(rid);
            for(Long dep: deps) {
                System.out.print(dep);
                System.out.print(" ,");
            }
            System.out.print("\n");
        }
    }

}
