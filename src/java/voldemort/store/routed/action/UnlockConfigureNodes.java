/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.store.routed.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.ZoneAffinity;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.UnlockPipelineData;
import voldemort.utils.ByteArray;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UnlockConfigureNodes extends
        AbstractConfigureNodes<Iterable<ByteArray>, Map<ByteArray, Boolean>, UnlockPipelineData> {

    private final Iterable<ByteArray> keys;

    private final Zone clientZone;

    private final ZoneAffinity zoneAffinity;

    public UnlockConfigureNodes(UnlockPipelineData pipelineData,
                                Event completeEvent,
                                FailureDetector failureDetector,
                                int requiredUnlock,
                                RoutingStrategy routingStrategy,
                                Iterable<ByteArray> keys,
                                Zone clientZone,
                                ZoneAffinity zoneAffinity) {
        super(pipelineData, completeEvent, failureDetector, requiredUnlock, routingStrategy);
        this.keys = keys;
        this.clientZone = clientZone;
        this.zoneAffinity = zoneAffinity;
    }

    @Override
    public void execute(Pipeline pipeline) {
        Map<Node, List<ByteArray>> nodeToKeysMap = Maps.newHashMap();
        Map<ByteArray, List<Node>> keyToExtraNodesMap = Maps.newHashMap();

        for(ByteArray key: keys) {
            List<Node> nodes = null;
            List<Node> originalNodes = null;

            try {
                originalNodes = getNodes(key);
            } catch(VoldemortException e) {
                pipelineData.setFatalError(e);
                pipeline.addEvent(Event.ERROR);
                return;
            }

            // TODO TEST REQUIRED VALUE
            List<Node> preferredNodes = Lists.newArrayListWithCapacity(required);
            List<Node> extraNodes = Lists.newArrayListWithCapacity(3);

            if(zoneAffinity != null && zoneAffinity.isGetAllOpZoneAffinityEnabled()) {
                nodes = new ArrayList<Node>();
                for(Node node: originalNodes) {
                    if(node.getZoneId() == clientZone.getId()) {
                        nodes.add(node);
                    }
                }
            } else {
                nodes = originalNodes;
            }

            if(pipelineData.getZonesRequired() != null) {

                if(pipelineData.getZonesRequired() > this.clientZone.getProximityList().size()) {
                    throw new VoldemortException("Number of zones required should be less than the total number of zones");
                }

                if(pipelineData.getZonesRequired() > required) {
                    throw new VoldemortException("Number of zones required should be less than the required number of "
                                                 + pipeline.getOperation().getSimpleName() + "s");
                }

                // Create zone id to node mapping
                Map<Integer, List<Node>> zoneIdToNode = new HashMap<Integer, List<Node>>();
                for(Node node: nodes) {
                    List<Node> nodesList = null;
                    if(zoneIdToNode.containsKey(node.getZoneId())) {
                        nodesList = zoneIdToNode.get(node.getZoneId());
                    } else {
                        nodesList = new ArrayList<Node>();
                        zoneIdToNode.put(node.getZoneId(), nodesList);
                    }
                    nodesList.add(node);
                }

                nodes = new ArrayList<Node>();
                LinkedList<Integer> proximityList = this.clientZone.getProximityList();
                // Add a node from every zone
                for(int index = 0; index < pipelineData.getZonesRequired(); index++) {
                    List<Node> zoneNodes = zoneIdToNode.get(proximityList.get(index));
                    if(zoneNodes != null) {
                        nodes.add(zoneNodes.remove(0));
                    }
                }

                // Add the rest
                List<Node> zoneIDNodeList = zoneIdToNode.get(this.clientZone.getId());
                if(zoneIDNodeList != null) {
                    nodes.addAll(zoneIDNodeList);
                }

                for(int index = 0; index < proximityList.size(); index++) {
                    List<Node> zoneNodes = zoneIdToNode.get(proximityList.get(index));
                    if(zoneNodes != null)
                        nodes.addAll(zoneNodes);
                }

            }

            for(Node node: nodes) {
                if(preferredNodes.size() < required)
                    preferredNodes.add(node);
                else
                    extraNodes.add(node);
            }

            for(Node node: preferredNodes) {
                List<ByteArray> nodeKeys = nodeToKeysMap.get(node);

                if(nodeKeys == null) {
                    nodeKeys = Lists.newArrayList();
                    nodeToKeysMap.put(node, nodeKeys);
                }

                nodeKeys.add(key);
            }

            if(!extraNodes.isEmpty()) {
                List<Node> list = keyToExtraNodesMap.get(key);

                if(list == null)
                    keyToExtraNodesMap.put(key, extraNodes);
                else
                    list.addAll(extraNodes);
            }
        }

        pipelineData.setKeyToExtraNodesMap(keyToExtraNodesMap);
        pipelineData.setNodeToKeysMap(nodeToKeysMap);

        pipeline.addEvent(completeEvent);
    }

}
