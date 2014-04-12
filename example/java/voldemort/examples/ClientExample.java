/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.examples;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

public class ClientExample {

    public static void main(String[] args) {
        // In real life this stuff would get wired in
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        StoreClient<String, BananaMsg.Banana> client = factory.getStoreClient("test");
        client.put("some_key",
                   BananaMsg.Banana.newBuilder().setTipo("costa rica").setNome("chiquita").build(),
                   0L);

        // get the value
        Versioned<BananaMsg.Banana> version = client.get("some_key", new RUD());
        System.out.println(version.getValue());
        // modify the value
        // version.setObject("new_value");

        // update the value
    }

}
