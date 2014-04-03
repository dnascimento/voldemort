package voldemort.store.contained;

import voldemort.client.protocol.pb.VProto;
import voldemort.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainingSerializer {

    /**
     * To database
     */
    public byte[] pack(byte[] object, long rid) {
        if(rid == 0) {
            return object;
        } else {
            System.out.println("Request writing: " + rid);
            return VProto.Container.newBuilder()
                                   .setData(ByteString.copyFrom(object))
                                   .setRid(rid)
                                   .build()
                                   .toByteArray();
        }
    }

    /**
     * Retrieve from database (unpack)
     */
    public Pair<Long, byte[]> unpack(byte[] bytes) {
        // This is were get dependencies can be founded
        try {
            VProto.Container c = VProto.Container.parseFrom(bytes);
            long rid = c.getRid();
            System.out.println("toObjectRid: " + rid);
            return new Pair<Long, byte[]>(rid, c.getData().toByteArray());
        } catch(InvalidProtocolBufferException e) {
            return new Pair<Long, byte[]>(0L, bytes);
        }
    }

}
