package voldemort.performance.benchmark;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.HashMap;

public class Measurement {

    private String name;

    public String getName() {
        return name;
    }

    private int buckets;
    private int[] histogram;
    private int histogramOverflow;
    private int windowOperations;
    private long windowTotalLatency;
    private int operations = 0;
    private int minLatency = -1;
    private int maxLatency = -1;
    private long totalLatency = 0;
    private HashMap<Integer, int[]> returnCodes;
    private boolean summaryOnly = false;

    public Measurement(String name, boolean summaryOnly) {
        this.name = name;
        this.buckets = 3000; // Default bucket size of 3000 milliseconds
        this.histogram = new int[buckets];
        this.histogramOverflow = 0;
        this.operations = 0;
        this.totalLatency = 0;
        this.windowOperations = 0;
        this.windowTotalLatency = 0;
        this.minLatency = -1;
        this.maxLatency = -1;
        this.returnCodes = new HashMap<Integer, int[]>();
        this.summaryOnly = summaryOnly;
    }

    public synchronized void recordReturnCode(int code) {
        Integer Icode = code;
        if(!returnCodes.containsKey(Icode)) {
            int[] val = new int[1];
            val[0] = 0;
            returnCodes.put(Icode, val);
        }
        returnCodes.get(Icode)[0]++;
    }

    public synchronized void recordLatency(int latency) {
        if(latency >= buckets) {
            histogramOverflow++;
        } else {
            histogram[latency]++;
        }
        operations++;
        totalLatency += latency;
        windowOperations++;
        windowTotalLatency += latency;

        if((minLatency < 0) || (latency < minLatency)) {
            minLatency = latency;
        }

        if((maxLatency < 0) || (latency > maxLatency)) {
            maxLatency = latency;
        }
    }

    public Results generateResults() {
        int median = 0, q95 = 0, q99 = 0;
        int opcounter = 0;
        boolean done95th = false, done50th = false;
        for(int i = 0; i < buckets; i++) {
            opcounter += histogram[i];
            double currentQuartile = ((double) opcounter) / ((double) operations);
            if(!done50th && currentQuartile >= 0.50) {
                median = i;
                done50th = true;
            }
            if(!done95th && currentQuartile >= 0.95) {
                q95 = i;
                done95th = true;
            }
            if(currentQuartile >= 0.99) {
                q99 = i;
                break;
            }
        }
        return new Results(operations, minLatency, maxLatency, totalLatency, median, q95, q99);
    }

    public void printReport(PrintStream out) {

        Results result = generateResults();
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(4);
        nf.setGroupingUsed(false);

        out.println("[" + getName() + "]\tOperations: " + operations);
        out.println("[" + getName() + "]\tAverage(ms): "
                    + nf.format((((double) totalLatency) / ((double) operations))));
        out.println("[" + getName() + "]\tMin(ms): " + nf.format(minLatency));
        out.println("[" + getName() + "]\tMax(ms): " + nf.format(maxLatency));
        out.println("[" + getName() + "]\tMedian(ms): " + nf.format(result.medianLatency));
        out.println("[" + getName() + "]\t95th(ms): " + nf.format(result.q95Latency));
        out.println("[" + getName() + "]\t99th(ms): " + nf.format(result.q99Latency));

        if(!this.summaryOnly) {
            for(Integer I: returnCodes.keySet()) {
                int[] val = returnCodes.get(I);
                out.println("[" + getName() + "]\tReturn: " + I + "\t" + val[0]);
            }

            for(int i = 0; i < buckets; i++) {
                out.println("[" + getName() + "]: " + i + "\t" + histogram[i]);
            }
            out.println("[" + getName() + "]: >" + buckets + "\t" + histogramOverflow);
        }
    }
}
