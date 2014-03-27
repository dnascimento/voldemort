package voldemort.undoTracker;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import voldemort.utils.ByteArray;

public class UndoStub {

    // TODO Check threading
    private static ConcurrentHashMap<ByteArray, LinkedList<Op>> trackLocalAccess = new ConcurrentHashMap<ByteArray, LinkedList<Op>>();

    /**
     * Each request handler has a UndoStub instance
     */
    public UndoStub() {
        SendOpTrack.init(trackLocalAccess);
    }

    public void get(ByteArray key, long rid) {
        if(rid != 0) {
            trackRequest(key, Op.OpType.Read, rid);
        }
        System.out.println(rid + " : get key: " + hexStringToAscii(key));
    }

    public void put(ByteArray key, long rid) {
        if(rid != 0) {
            trackRequest(key, Op.OpType.Write, rid);
        }
        System.out.println(rid + " : put key: " + hexStringToAscii(key));
    }

    public void delete(ByteArray key, long rid) {
        if(rid != 0) {
            trackRequest(key, Op.OpType.Delete, rid);
        }
        System.out.println(rid + " : delete key: " + hexStringToAscii(key));
    }

    private void trackRequest(ByteArray key, Op.OpType type, long rid) {
        if(rid == 0)
            return;

        LinkedList<Op> list = trackLocalAccess.get(key);
        // TODO synchronize: if both get the same key at same time
        if(list == null) {
            System.out.println("List is null");
            list = new LinkedList<Op>();
            trackLocalAccess.put(key, list);
        }
        list.addLast(new Op(rid, type));
    }

    private String hexStringToAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return key.toString();
    }
}
