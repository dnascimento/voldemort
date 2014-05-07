package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;

public class UnlockClientRequest extends AbstractStoreClientRequest<Map<ByteArray, Boolean>> {

    private final Iterable<ByteArray> keys;

    public UnlockClientRequest(String storeName,
                               RequestFormat requestFormat,
                               RequestRoutingType requestRoutingType,
                               Iterable<ByteArray> keys,
                               RUD rud) {
        super(storeName, requestFormat, requestRoutingType, rud);
        this.keys = keys;
    }

    @Override
    public boolean isCompleteResponse(ByteBuffer buffer) {
        return requestFormat.isCompleteUnlockRequest(buffer);
    }

    @Override
    protected void formatRequestInternal(DataOutputStream outputStream) throws IOException {
        requestFormat.writeUnlockRequest(outputStream, storeName, keys, requestRoutingType, rud);
    }

    @Override
    protected Map<ByteArray, Boolean> parseResponseInternal(DataInputStream inputStream)
            throws IOException {
        return requestFormat.readUnlockResponse(inputStream);
    }

}
