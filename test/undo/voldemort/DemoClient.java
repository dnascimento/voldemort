package voldemort;

import java.util.concurrent.TimeUnit;

import pt.inesc.ask.proto.AskProto;
import pt.inesc.ask.proto.AskProto.Comment;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.TimeoutConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.undoTracker.SRD;

public class DemoClient {

    private static final int N_TIMES = 1000000;
    private static final long RID = System.currentTimeMillis();
    private static final String KEY = "dario";
    private StoreClient<String, AskProto.Comment> store;
    private final String storeName = "commentStore";
    private final String bootstrapUrl = "tcp://localhost:6666";

    public static void main(String[] args) {
        DemoClient client = new DemoClient();
        long start = System.nanoTime();
        client.put(N_TIMES);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
    }

    private DemoClient() {
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                                    .setRequestFormatType(RequestFormatType.PROTOCOL_BUFFERS)
                                                                                    .setEnableJmx(true)
                                                                                    .setSocketKeepAlive(true)
                                                                                    .setMaxBootstrapRetries(20)
                                                                                    .setConnectionTimeout(200000000,
                                                                                                          TimeUnit.MILLISECONDS)
                                                                                    .setFailureDetectorAsyncRecoveryInterval(2000000)
                                                                                    .setFailureDetectorThresholdCountMinimum(2000000)
                                                                                    .setFailureDetectorRequestLengthThreshold(10000)
                                                                                    .setRoutingTimeout(2000000,
                                                                                                       TimeUnit.MILLISECONDS)
                                                                                    // operation
                                                                                    // timeout
                                                                                    .setTimeoutConfig(new TimeoutConfig(50000000))
                                                                                    .setSocketTimeout(2000000,
                                                                                                      TimeUnit.MILLISECONDS));

        store = factory.getStoreClient(storeName);
    }

    private void put(int nTimes) {
        Comment com = Comment.newBuilder()
                             .setText("darioooooooooooooooo")
                             .setAuthor("baba")
                             .build();
        SRD srd = new SRD(RID, 0, false);
        long sum = 0;
        long[] val = new long[nTimes];
        for(int i = 0; i < nTimes; i++) {
            long start = System.nanoTime();
            store.put(KEY, com, srd);
            val[i] = System.nanoTime() - start;
        }

        for(int i = 0; i < nTimes; i++) {
            sum += val[i];
        }
        long avg = sum / nTimes;
        long e = 0;
        for(int i = 0; i < nTimes; i++) {
            e += (val[i] - avg) * (val[i] - avg);
        }

        System.out.println("stdDev: " + Math.sqrt((e / avg)));
        System.out.println("average: (nano sec) " + avg);
        System.out.println("rate: (nano sec) " + (nTimes / (sum / 1000000)));

    }
}
