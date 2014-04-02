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

package voldemort.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.JmxUtils;
import voldemort.utils.Utils;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * The default {@link voldemort.client.StoreClient StoreClient} implementation
 * you get back from a {@link voldemort.client.StoreClientFactory
 * StoreClientFactory}
 * 
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
@Threadsafe
@JmxManaged(description = "A voldemort client")
public class DefaultStoreClient<K, V> implements StoreClient<K, V> {

    private final Logger logger = Logger.getLogger(DefaultStoreClient.class);
    protected Callable<Object> beforeRebootstrapCallback = null;
    protected StoreClientFactory storeFactory;
    protected int metadataRefreshAttempts;
    protected String storeName;
    protected InconsistencyResolver<Versioned<V>> resolver;
    protected volatile Store<K, V, Object> store;

    public DefaultStoreClient(String storeName,
                              InconsistencyResolver<Versioned<V>> resolver,
                              StoreClientFactory storeFactory,
                              int maxMetadataRefreshAttempts) {
        this.storeName = Utils.notNull(storeName);
        this.resolver = resolver;
        this.storeFactory = Utils.notNull(storeFactory);
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;

        // Registering self to be able to bootstrap client dynamically via JMX
        JmxUtils.registerMbean(this,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.getClass())
                                                                 + "." + storeName));

        bootStrap();
    }

    // Default constructor invoked from child class
    public DefaultStoreClient() {}

    @JmxOperation(description = "bootstrap metadata from the cluster.")
    public void bootStrap() {
        if(beforeRebootstrapCallback != null) {
            try {
                beforeRebootstrapCallback.call();
            } catch(Exception e) {
                logger.warn("Exception caught when running callback before bootstrap", e);
            }
        }
        logger.info("Bootstrapping metadata for store " + this.storeName);
        this.store = storeFactory.getRawStore(storeName, resolver);
    }

    @Override
    public boolean delete(K key, long rid) {
        Versioned<V> versioned = get(key, rid);
        if(versioned == null)
            return false;
        return delete(key, versioned.getVersion(), rid);
    }

    @Override
    public boolean delete(K key, Version version, long rid) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return store.delete(key, version, rid);
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during delete [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    @Override
    public V getValue(K key, V defaultValue, long rid) {
        Versioned<V> versioned = get(key, rid);
        if(versioned == null)
            return defaultValue;
        else
            return versioned.getValue();
    }

    @Override
    public V getValue(K key, long rid) {
        Versioned<V> returned = get(key, null, rid);
        if(returned == null)
            return null;
        else
            return returned.getValue();
    }

    @Override
    public Versioned<V> get(K key, Versioned<V> defaultValue, long rid) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                List<Versioned<V>> items = store.get(key, null, rid);
                return getItemOrThrow(key, defaultValue, items);
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during get [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public Versioned<V> get(K key, Versioned<V> defaultValue, Object transform, long rid) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                List<Versioned<V>> items = store.get(key, transform, rid);
                return getItemOrThrow(key, defaultValue, items);
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during get [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    protected List<Version> getVersions(K key, long rid) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return store.getVersions(key, rid);
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during getVersions [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    protected Versioned<V> getItemOrThrow(K key, Versioned<V> defaultValue, List<Versioned<V>> items) {
        if(items.size() == 0)
            return defaultValue;
        else if(items.size() == 1)
            return items.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + items, items);
    }

    @Override
    public Versioned<V> get(K key, long rid) {
        return get(key, null, rid);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, long rid) {
        Map<K, List<Versioned<V>>> items = null;
        for(int attempts = 0;; attempts++) {
            if(attempts >= this.metadataRefreshAttempts)
                throw new VoldemortException(this.metadataRefreshAttempts
                                             + " metadata refresh attempts failed.");
            try {
                items = store.getAll(keys, null, rid);
                break;
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during getAll [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    @Override
    public Version put(K key, V value, long rid) {
        List<Version> versions = getVersions(key, rid);
        Versioned<V> versioned;
        if(versions.isEmpty())
            versioned = Versioned.value(value, new VectorClock());
        else if(versions.size() == 1)
            versioned = Versioned.value(value, versions.get(0));
        else {
            versioned = get(key, null, rid);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
        }
        return put(key, versioned, rid);
    }

    public Version put(K key, Versioned<V> versioned, Object transform, long rid)
            throws ObsoleteVersionException {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                store.put(key, versioned, transform, rid);
                return versioned.getVersion();
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during put [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    @Override
    public boolean putIfNotObsolete(K key, Versioned<V> versioned, long rid) {
        try {
            put(key, versioned, rid);
            return true;
        } catch(ObsoleteVersionException e) {
            return false;
        }
    }

    @Override
    public Version put(K key, Versioned<V> versioned, long rid) throws ObsoleteVersionException {

        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                store.put(key, versioned, null, rid);
                return versioned.getVersion();
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during put [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    @Override
    public boolean applyUpdate(UpdateAction<K, V> action, long rid) {
        return applyUpdate(action, 3, rid);
    }

    @Override
    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries, long rid) {
        boolean success = false;
        try {
            for(int i = 0; i < maxTries; i++) {
                try {
                    action.update(this);
                    success = true;
                    return success;
                } catch(ObsoleteVersionException e) {
                    // ignore for now
                }
            }
        } finally {
            if(!success)
                action.rollback();
        }

        // if we got here we have seen too many ObsoleteVersionExceptions
        // and have rolled back the updates
        return false;
    }

    @Override
    public List<Node> getResponsibleNodes(K key) {
        RoutingStrategy strategy = (RoutingStrategy) store.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
        @SuppressWarnings("unchecked")
        Serializer<K> keySerializer = (Serializer<K>) store.getCapability(StoreCapabilityType.KEY_SERIALIZER);
        return strategy.routeRequest(keySerializer.toBytes(key));
    }

    protected Version getVersion(K key, long rid) {
        List<Version> versions = getVersions(key, rid);
        if(versions.size() == 0)
            return null;
        else if(versions.size() == 1)
            return versions.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + versions, versions);
    }

    @Override
    public Versioned<V> get(K key, Object transforms, long rid) {
        return get(key, null, transforms, rid);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms, long rid) {
        Map<K, List<Versioned<V>>> items = null;
        for(int attempts = 0;; attempts++) {
            if(attempts >= this.metadataRefreshAttempts)
                throw new VoldemortException(this.metadataRefreshAttempts
                                             + " metadata refresh attempts failed.");
            try {
                items = store.getAll(keys, transforms, rid);
                break;
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during getAll [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    @Override
    public Version put(K key, V value, Object transforms, long rid) {
        List<Version> versions = getVersions(key, rid);
        Versioned<V> versioned;
        if(versions.isEmpty())
            versioned = Versioned.value(value, new VectorClock());
        else if(versions.size() == 1)
            versioned = Versioned.value(value, versions.get(0));
        else {
            versioned = get(key, null, rid);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
        }
        return put(key, versioned, transforms, rid);

    }

    public void setBeforeRebootstrapCallback(Callable<Object> callback) {
        beforeRebootstrapCallback = callback;
    }
}
