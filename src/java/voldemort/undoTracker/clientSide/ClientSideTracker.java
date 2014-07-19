/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */
package voldemort.undoTracker.clientSide;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.TrackEntry;
import undo.proto.ToManagerProto.TrackMsg;
import voldemort.undoTracker.RUD;

import com.google.common.collect.ArrayListMultimap;

public class ClientSideTracker extends Thread {

    private static final Logger log = LogManager.getLogger(ClientSideTracker.class.getName());

    int period = 5000;
    private ArrayListMultimap<Long, Long> dependencyPerRid = ArrayListMultimap.create();
    Socket s;

    public ClientSideTracker() {
        super();
        try {
            s = new Socket("localhost", 9090);
        } catch(UnknownHostException e) {
            log.error(e);
        } catch(IOException e) {
            log.error(e);
        }
    }

    public synchronized void trackGet(RUD rud, RUD dependentRud) {
        dependencyPerRid.put(rud.rid, dependentRud.rid);
    }

    public synchronized ArrayListMultimap<Long, Long> extractDependencies() {
        if(dependencyPerRid.size() != 0) {
            ArrayListMultimap<Long, Long> old = dependencyPerRid;
            dependencyPerRid = ArrayListMultimap.create();
            return old;
        } else {
            return null;
        }
    }

    @Override
    public void run() {
        while(true) {
            ArrayListMultimap<Long, Long> map = extractDependencies();
            try {
                if(map != null)
                    send(map);
                sleep(period);
            } catch(UnknownHostException e) {
                log.error(e);
            } catch(IOException e) {
                log.error(e);
            } catch(InterruptedException e) {
                log.error(e);
            }
        }
    }

    private void send(ArrayListMultimap<Long, Long> map) throws UnknownHostException, IOException {
        TrackMsg.Builder mB = ToManagerProto.TrackMsg.newBuilder();
        for(Long k: map.keySet()) {
            TrackEntry t = TrackEntry.newBuilder().setRid(k).addAllDependency(map.get(k)).build();
            mB.addEntry(t);
        }
        ToManagerProto.MsgToManager m = ToManagerProto.MsgToManager.newBuilder()
                                                                   .setTrackMsgFromClient(mB)
                                                                   .build();
        m.writeDelimitedTo(s.getOutputStream());
    }
}
