package voldemort.store.contained;

import voldemort.client.protocol.pb.VProto;
import voldemort.undoTracker.SRD;
import voldemort.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainingSerializer {

    /**
     * To database
     */
    public byte[] pack(byte[] object, SRD srd) {
        if(srd.rid == 0) {
            return object;
        } else {
            return VProto.Container.newBuilder()
                                   .setData(ByteString.copyFrom(object))
                                   .setRud(srd.toProto())
                                   .build()
                                   .toByteArray();
        }
    }

    /**
     * Retrieve from database (unpack)
     */
    public Pair<SRD, byte[]> unpack(byte[] bytes) {
        // This is were get dependencies can be founded
        try {
            VProto.Container c = VProto.Container.parseFrom(bytes);
            SRD srd = new SRD(c.getRud());
            return new Pair<SRD, byte[]>(srd, c.getData().toByteArray());
        } catch(InvalidProtocolBufferException e) {
            return new Pair<SRD, byte[]>(new SRD(), bytes);
        }
    }

}
