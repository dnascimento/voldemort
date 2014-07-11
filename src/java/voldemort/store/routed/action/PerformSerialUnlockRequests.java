/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.store.routed.action;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.store.routed.UnlockPipelineData;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;

public class PerformSerialUnlockRequests extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, Boolean>, UnlockPipelineData> {

    private final Iterable<ByteArray> keys;

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    private final int required;

    private RUD rud;

    public PerformSerialUnlockRequests(UnlockPipelineData pipelineData,
                                       Event completeEvent,
                                       Iterable<ByteArray> keys,
                                       FailureDetector failureDetector,
                                       Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                       int required,
                                       RUD rud) {
        super(pipelineData, completeEvent);
        this.keys = keys;
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.required = required;
        this.rud = rud;
    }

    @Override
    public void execute(Pipeline pipeline) {
        Map<ByteArray, Boolean> result = pipelineData.getResult();

        for(ByteArray key: keys) {
            boolean zoneRequirement = false;
            MutableInt successCount = pipelineData.getSuccessCount(key);

            if(logger.isDebugEnabled())
                logger.debug("Unlock for key " + ByteUtils.toHexString(key.get()) + " (keyRef: "
                             + System.identityHashCode(key) + ") successes: "
                             + successCount.intValue() + " required: " + required);

            // TODO check
            if(successCount.intValue() >= required) {
                if(pipelineData.getZonesRequired() != null && pipelineData.getZonesRequired() > 0) {

                    if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        int zonesSatisfied = pipelineData.getKeyToZoneResponse().get(key).size();
                        if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                            continue;
                        } else {
                            zoneRequirement = true;
                        }
                    } else {
                        zoneRequirement = true;
                    }

                } else {
                    continue;
                }
            }

            List<Node> extraNodes = pipelineData.getKeyToExtraNodesMap().get(key);

            if(extraNodes == null)
                continue;

            for(Node node: extraNodes) {
                long start = System.nanoTime();

                try {
                    Store<ByteArray, byte[], byte[]> store = stores.get(node.getId());
                    ArrayList<ByteArray> keyIt = new ArrayList<ByteArray>();
                    keyIt.add(key);
                    Map<ByteArray, Boolean> keyResult = store.unlockKeys(keyIt, rud);
                    result.put(key, keyResult.get(key));

                    Response<Iterable<ByteArray>, Map<ByteArray, Boolean>> response = new Response<Iterable<ByteArray>, Map<ByteArray, Boolean>>(node,
                                                                                                                                                 keyIt,
                                                                                                                                                 keyResult,
                                                                                                                                                 ((System.nanoTime() - start) / Time.NS_PER_MS));

                    successCount.increment();
                    pipelineData.getResponses().add(response);
                    failureDetector.recordSuccess(response.getNode(), response.getRequestTime());

                    if(logger.isDebugEnabled())
                        logger.debug("Unlock for key " + ByteUtils.toHexString(key.get())
                                     + " (keyRef: " + System.identityHashCode(key)
                                     + ") successes: " + successCount.intValue() + " required: "
                                     + required + " new Unlock success on node " + node.getId());

                    HashSet<Integer> zoneResponses = null;
                    if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        zoneResponses = pipelineData.getKeyToZoneResponse().get(key);
                    } else {
                        zoneResponses = new HashSet<Integer>();
                        pipelineData.getKeyToZoneResponse().put(key, zoneResponses);
                    }
                    zoneResponses.add(response.getNode().getZoneId());

                    if(zoneRequirement) {
                        if(zoneResponses.size() >= pipelineData.getZonesRequired())
                            break;
                    } else {
                        if(successCount.intValue() >= required)
                            break;
                    }

                } catch(Exception e) {
                    long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                    if(handleResponseError(e, node, requestTime, pipeline, failureDetector))
                        return;
                }
            }
        }

        for(ByteArray key: keys) {
            MutableInt successCount = pipelineData.getSuccessCount(key);

            if(successCount.intValue() < required) {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                             + " "
                                                                                             + pipeline.getOperation()
                                                                                                       .getSimpleName()
                                                                                             + "s required, but "
                                                                                             + successCount.intValue()
                                                                                             + " succeeded. Failing nodes : "
                                                                                             + pipelineData.getFailedNodes(),
                                                                                     pipelineData.getFailures()));
                pipeline.addEvent(Event.ERROR);
                return;
            }
        }

        pipeline.addEvent(completeEvent);
    }
}
