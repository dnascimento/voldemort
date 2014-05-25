package voldemort.redoAndSnapshot;

import java.util.Arrays;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;

public class UnlockStoreTest {

    private void unlockKey(String storeName, RUD rud, ByteArray... keys) {
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://localhost:6666")
                                                                                    .setRequestFormatType(RequestFormatType.PROTOCOL_BUFFERS));
        StoreClient<ByteArray, Object> s = factory.getStoreClient(storeName);
        s.unlockKeys(Arrays.asList(keys), rud);
    }
}
