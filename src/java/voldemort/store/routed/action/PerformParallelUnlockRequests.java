/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.store.routed.action;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InvalidMetadataException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.store.routed.UnlockPipelineData;
import voldemort.undoTracker.SRD;
import voldemort.utils.ByteArray;

public class PerformParallelUnlockRequests extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, Boolean>, UnlockPipelineData> {

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final SRD srd;

    private Event insufficientSuccesses;

    public PerformParallelUnlockRequests(UnlockPipelineData pipelineData,
                                         Event completeEvent,
                                         FailureDetector failureDetector,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStores,
                                         Event insufficientSuccesses,
                                         SRD srd) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.srd = srd;
        this.insufficientSuccesses = insufficientSuccesses;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(final Pipeline pipeline) {
        int attempts = pipelineData.getNodeToKeysMap().size();

        final Map<Integer, Response<Iterable<ByteArray>, Object>> responses = new ConcurrentHashMap<Integer, Response<Iterable<ByteArray>, Object>>();
        final CountDownLatch latch = new CountDownLatch(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        for(Map.Entry<Node, List<ByteArray>> entry: pipelineData.getNodeToKeysMap().entrySet()) {
            final Node node = entry.getKey();
            final Collection<ByteArray> keys = entry.getValue();

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                @Override
                public void requestComplete(Object result, long requestTime) {
                    if(logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                     + " response received (" + requestTime + " ms.) from node "
                                     + node.getId());

                    Response<Iterable<ByteArray>, Object> response = new Response<Iterable<ByteArray>, Object>(node,
                                                                                                               keys,
                                                                                                               result,
                                                                                                               requestTime);
                    responses.put(node.getId(), response);
                    latch.countDown();

                    // Note errors that come in after the pipeline has finished.
                    // These will *not* get a chance to be called in the loop of
                    // responses below.
                    if(pipeline.isFinished() && response.getValue() instanceof Exception)
                        if(response.getValue() instanceof InvalidMetadataException) {
                            pipelineData.reportException((InvalidMetadataException) response.getValue());
                            logger.warn("Received invalid metadata problem after a successful "
                                        + pipeline.getOperation().getSimpleName()
                                        + " call on node " + node.getId() + ", store '"
                                        + pipelineData.getStoreName() + "'");
                        } else {
                            handleResponseError(response, pipeline, failureDetector);
                        }
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitUnlockRequest(keys, callback, timeoutMs, srd);
        }

        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        for(Response<Iterable<ByteArray>, Object> response: responses.values()) {
            if(response.getValue() instanceof Exception) {
                if(handleResponseError(response, pipeline, failureDetector))
                    return;
            } else {
                Map<ByteArray, Boolean> values = (Map<ByteArray, Boolean>) response.getValue();

                for(ByteArray key: response.getKey()) {
                    MutableInt successCount = pipelineData.getSuccessCount(key);
                    successCount.increment();

                    // store the results
                    Boolean keyResult = values.get(key);
                    pipelineData.getResult().put(key, keyResult);

                    HashSet<Integer> zoneResponses = null;
                    if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        zoneResponses = pipelineData.getKeyToZoneResponse().get(key);
                    } else {
                        zoneResponses = new HashSet<Integer>();
                        pipelineData.getKeyToZoneResponse().put(key, zoneResponses);
                    }
                    zoneResponses.add(response.getNode().getZoneId());
                }

                pipelineData.getResponses()
                            .add(new Response<Iterable<ByteArray>, Map<ByteArray, Boolean>>(response.getNode(),
                                                                                            response.getKey(),
                                                                                            values,
                                                                                            response.getRequestTime()));
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
            }
        }
        // TODO if not sufficient success:
        // pipeline.addEvent(insufficientSuccesses)
        pipeline.addEvent(completeEvent);
    }
}
