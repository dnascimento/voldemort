package voldemort.store.contained;

import voldemort.client.protocol.pb.VProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainingSerializer {

    /**
     * Send to database (pack)
     */
    public byte[] toBytes(byte[] object, long rid) {
        if(rid == 0) {
            return object;
        } else {
            System.out.println("toBytesRid: " + rid);
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
    public byte[] toObject(byte[] bytes) {
        try {
            VProto.Container c = VProto.Container.parseFrom(bytes);
            long rid = c.getRid();
            System.out.println("toObjectRid: " + rid);
            return c.getData().toByteArray();
        } catch(InvalidProtocolBufferException e) {
            return bytes;
        }
    }

}
