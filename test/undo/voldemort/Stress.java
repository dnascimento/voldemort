package voldemort;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;

import voldemort.performance.benchmark.Benchmark;
import voldemort.performance.benchmark.Metrics;
import voldemort.performance.benchmark.Results;
import voldemort.utils.Props;

public class Stress {

    public static void main(String[] args) throws Exception {
        Benchmark benchmark = new Benchmark();

        // Write to file "latencyVsThreads.txt" which we can use to plot
        PrintStream outputStream = new PrintStream(new File("latencyVsThreads.txt"));

        Props workLoadProps = new Props();
        workLoadProps.put("record-count", 1000000); // Insert million records
                                                    // during warm-up phase
        workLoadProps.put("ops-count", 1000000); // Run million operations
                                                 // during benchmark phase
        workLoadProps.put("r", 95); // Read intensive program with 95% read ...
        workLoadProps.put("w", 5); // ...and 5% writes ...
        workLoadProps.put("record-selection", "uniform"); // ...with keys being
                                                          // selected using
                                                          // uniform
                                                          // distribution

        // Run tool on Voldemort server running on localhost with store-name
        // "read-intensive"
        workLoadProps.put("url", "tcp://localhost:6666");
        workLoadProps.put("store-name", "test");

        // Initialize benchmark
        benchmark.initializeStore(workLoadProps.with("threads", 100));
        benchmark.initializeWorkload(workLoadProps);

        // Run the warm-up phase
        long warmUpRunTime = benchmark.runTests(false);

        // Change the number of client threads and capture the median latency
        for(int threads = 10; threads <= 100; threads += 10) {
            benchmark.initializeStore(workLoadProps.with("threads", threads));
            benchmark.initializeWorkload(workLoadProps);

            long benchmarkRunTime = benchmark.runTests(true);
            HashMap<String, Results> resultsMap = Metrics.getInstance().getResults();
            System.out.println(resultsMap.keySet());
            // if(resultsMap.containsKey(VoldemortWrapper.READS_STRING)) {
            // Results result = resultsMap.get(VoldemortWrapper.READS_STRING);
            // outputStream.println(VoldemortWrapper.READS_STRING + "\t" +
            // String.valueOf(threads)
            // + "\t" + result.medianLatency);
            // }
        }

        // Close the benchmark
        benchmark.close();
    }
}
