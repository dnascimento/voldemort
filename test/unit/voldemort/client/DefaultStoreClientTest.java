/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.undoTracker.SRD;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class DefaultStoreClientTest {

    protected int nodeId;
    protected Time time;
    protected StoreClient<String, String> client;

    @Before
    public void setUp() {
        this.nodeId = 0;
        this.time = SystemTime.INSTANCE;
        Serializer<String> serializer = new StringSerializer();
        MockStoreClientFactory factory = new MockStoreClientFactory(serializer,
                                                                    serializer,
                                                                    null,
                                                                    serializer,
                                                                    nodeId,
                                                                    time);
        this.client = factory.getStoreClient("test");
    }

    @Test
    public void testGet() {
        assertEquals("GET of non-existant key should return null.",
                     null,
                     client.get("k", new SRD()));
        client.put("k", "v", new SRD());
        assertEquals("After a PUT get should return the value", "v", client.get("k", new SRD())
                                                                           .getValue());
        assertNotNull("The version of the value found should be non-null",
                      client.get("k", new SRD()).getVersion());
    }

    @Test
    public void testGetWithDefault() {
        assertEquals("GET of missing key should return default.",
                     new Versioned<String>("v"),
                     client.get("k", new Versioned<String>("v"), new SRD()));
        assertEquals("null should be an acceptable default value.",
                     null,
                     client.getValue("k", null, new SRD()));
        client.put("k", "v", new SRD());
        assertEquals("If there is a value for k, get(k) should return it.",
                     new Versioned<String>("v",
                                           new VectorClock().incremented(nodeId,
                                                                         time.getMilliseconds())),
                     client.get("k", new Versioned<String>("v2"), new SRD()));
        assertNotNull(client.get("k", new SRD()).getVersion());
    }

    @Test
    public void testGetUnversioned() {
        assertEquals("GET of non-existant key should be null.",
                     null,
                     client.getValue("k", new SRD()));
        client.put("k", "v", new SRD());
        assertEquals("GET of k should return v, if v is there.",
                     "v",
                     client.getValue("k", new SRD()));
    }

    @Test
    public void testGetUnversionedWithDefault() {
        assertEquals("GET of non-existant key should return default.",
                     "v",
                     client.getValue("k", "v", new SRD()));
        assertEquals("null should be an acceptable default",
                     null,
                     client.getValue("k", null, new SRD()));
        client.put("k", "v", new SRD());
        assertEquals("default should not be returned if value is present.",
                     "v",
                     client.getValue("k", "v2", new SRD()));
    }

    @Test
    public void testPutVersioned() {
        client.put("k", Versioned.value("v"), new SRD());
        Versioned<String> v = client.get("k", new SRD());
        assertEquals("GET should return the version set by PUT.", "v", v.getValue());
        VectorClock expected = new VectorClock();
        expected.incrementVersion(nodeId, time.getMilliseconds());
        assertEquals("The version should be incremented after a put.", expected, v.getVersion());
        try {
            client.put("k", Versioned.value("v"), new SRD());
            fail("Put of obsolete version should throw exception.");
        } catch(ObsoleteVersionException e) {
            // this is good
        }
        // PUT of a concurrent version should succeed
        client.put("k",
                   new Versioned<String>("v2",
                                         new VectorClock().incremented(nodeId + 1,
                                                                       time.getMilliseconds())),
                   new SRD());
        assertEquals("GET should return the new value set by PUT.",
                     "v2",
                     client.getValue("k", new SRD()));
        assertEquals("GET should return the new version set by PUT.",
                     expected.incremented(nodeId + 1, time.getMilliseconds()),
                     client.get("k", new SRD()).getVersion());
    }

    @Test
    public void testPutUnversioned() {
        client.put("k", "v", new SRD());
        assertEquals("GET should fetch the value set by PUT", "v", client.getValue("k", new SRD()));
        client.put("k", "v2", new SRD());
        assertEquals("Overwrite of value should succeed.", "v2", client.getValue("k", new SRD()));
    }

    @Test
    public void testPutIfNotObsolete() {
        client.putIfNotObsolete("k", new Versioned<String>("v"), new SRD());
        assertEquals("PUT of non-obsolete version should succeed.",
                     "v",
                     client.getValue("k", new SRD()));
        assertFalse(client.putIfNotObsolete("k", new Versioned<String>("v2"), new SRD()));
        assertEquals("Failed PUT should not change the value stored.",
                     "v",
                     client.getValue("k", new SRD()));
    }

    @Test
    public void testDelete() {
        assertFalse("Delete of non-existant key should be false.", client.delete("k", new SRD()));
        client.put("k", "v", new SRD());
        assertTrue("Delete of contained key should be true", client.delete("k", new SRD()));
        assertNull("After a successful delete(k), get(k) should return null.",
                   client.get("k", new SRD()));
    }

    @Test
    public void testDeleteVersion() {
        assertFalse("Delete of non-existant key should be false.",
                    client.delete("k", new VectorClock(), new SRD()));
        client.put("k", new Versioned<String>("v"), new SRD());
        assertFalse("Delete of a lesser version should be false.",
                    client.delete("k", new VectorClock(), new SRD()));
        assertNotNull("After failed delete, value should still be there.",
                      client.get("k", new SRD()));
        assertTrue("Delete of k, with the current version should succeed.",
                   client.delete("k",
                                 new VectorClock().incremented(nodeId, time.getMilliseconds()),
                                 new SRD()));
        assertNull("After a successful delete(k), get(k) should return null.",
                   client.get("k", new SRD()));
    }

    @Test
    public void testGetAll() {
        client.put("k", "v", new SRD());
        client.put("l", "m", new SRD());
        client.put("a", "b", new SRD());

        Map<String, Versioned<String>> result = client.getAll(Arrays.asList("k", "l"), new SRD());
        assertEquals(2, result.size());
        assertEquals("v", result.get("k").getValue());
        assertEquals("m", result.get("l").getValue());

        result = client.getAll(Arrays.asList("m", "s"), new SRD());
        assertNotNull(client.get("k", new SRD()).getVersion());
        assertEquals(0, result.size());
    }
}
