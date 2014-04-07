package voldemort.undoTracker;

import java.io.UnsupportedEncodingException;

import voldemort.undoTracker.map.MultimapSync;
import voldemort.utils.ByteArray;

public class DBUndoStub {

    private static MultimapSync<ByteArray, Op> trackLocalAccess;
    private static InvertDependenciesAndSendThread sender;

    private synchronized void init() {
        if(sender != null)
            return;
        trackLocalAccess = new MultimapSync<ByteArray, Op>();
        sender = new InvertDependenciesAndSendThread(trackLocalAccess);
        sender.start();
    }

    /**
     * Each request handler has a UndoStub instance
     */
    public DBUndoStub() {
        if(sender == null)
            init();
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

        trackLocalAccess.put(key, new Op(rid, type));
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
