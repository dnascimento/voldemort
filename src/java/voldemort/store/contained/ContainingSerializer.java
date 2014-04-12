package voldemort.store.contained;

import voldemort.client.protocol.pb.VProto;
import voldemort.undoTracker.RUD;
import voldemort.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainingSerializer {

    /**
     * To database
     */
    public byte[] pack(byte[] object, RUD rud) {
        if(rud.rid == 0) {
            return object;
        } else {
            System.out.println("Request writing: " + rud.rid);
            return VProto.Container.newBuilder()
                                   .setData(ByteString.copyFrom(object))
                                   .setRud(rud.toProto())
                                   .build()
                                   .toByteArray();
        }
    }

    /**
     * Retrieve from database (unpack)
     */
    public Pair<RUD, byte[]> unpack(byte[] bytes) {
        // This is were get dependencies can be founded
        try {
            VProto.Container c = VProto.Container.parseFrom(bytes);
            RUD rud = new RUD(c.getRud());
            System.out.println("toObjectRid: " + rud);
            return new Pair<RUD, byte[]>(rud, c.getData().toByteArray());
        } catch(InvalidProtocolBufferException e) {
            return new Pair<RUD, byte[]>(new RUD(), bytes);
        }
    }

}
