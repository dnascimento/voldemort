/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.performance.benchmark;


public class Results {

    public int operations;
    public long totalLatency, minLatency, maxLatency, q99Latency, q95Latency, medianLatency;

    public Results(int ops, long minL, long maxL, long totalLat, long medL, long q95, long q99) {
        this.operations = ops;
        this.minLatency = minL;
        this.maxLatency = maxL;
        this.totalLatency = totalLat;
        this.medianLatency = medL;
        this.q99Latency = q99;
        this.q95Latency = q95;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("Operations = " + operations + "\n");
        buffer.append("Min Latency = " + minLatency + "\n");
        buffer.append("Max latency = " + maxLatency + "\n");
        buffer.append("Median Latency = " + medianLatency + "\n");
        buffer.append("95th percentile Latency = " + q95Latency + "\n");
        buffer.append("99th percentile Latency = " + q99Latency + "\n");
        return buffer.toString();
    }
}
