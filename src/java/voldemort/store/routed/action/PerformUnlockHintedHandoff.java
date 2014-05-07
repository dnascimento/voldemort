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

package voldemort.store.routed.action;

import java.util.Date;
import java.util.Set;

import voldemort.cluster.Node;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;

public class PerformUnlockHintedHandoff extends
        AbstractHintedHandoffAction<Boolean, BasicPipelineData<Boolean>> {

    public PerformUnlockHintedHandoff(BasicPipelineData<Boolean> pipelineData,
                                      Pipeline.Event completeEvent,
                                      Set<ByteArray> keys,
                                      HintedHandoff hintedHandoff,
                                      RUD rud) {
        super(pipelineData, completeEvent, keys, hintedHandoff, rud);
    }

    @Override
    public void execute(Pipeline pipeline) {
        for(Node failedNode: failedNodes) {
            int failedNodeId = failedNode.getId();
            if(logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + failedNode + ", store "
                             + pipelineData.getStoreName() + "key " + key);

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.UNLOCK,
                                 key,
                                 null,
                                 null,
                                 failedNodeId,
                                 new Date());
            // TODO may cause errors
            hintedHandoff.sendHintParallel(failedNode, null, slop);
        }
        pipeline.addEvent(completeEvent);
    }
}
