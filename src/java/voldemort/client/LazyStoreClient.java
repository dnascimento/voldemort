/*
 * Copyright 2008-2011 LinkedIn, Inc
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
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.undoTracker.SRD;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A {@link StoreClient} with lazy initialization. This is useful existing codes
 * that initializes clients at service deployment time, when the servers may or
 * may not be available
 * 
 */
public class LazyStoreClient<K, V> implements StoreClient<K, V> {

    private final Logger logger = Logger.getLogger(LazyStoreClient.class);
    private final Callable<StoreClient<K, V>> storeClientThunk;
    private StoreClient<K, V> storeClient;

    public LazyStoreClient(Callable<StoreClient<K, V>> storeClientThunk) {
        this(storeClientThunk, true);
    }

    /**
     * A Hybrid store client which tries to do immediate bootstrap. In case of
     * an exception, we fallback to the lazy way of doing initialization.
     * 
     * @param storeClientThunk The callback invoked for doing the actual
     *        bootstrap
     * @param instantInit A boolean flag when set indicates that we should try
     *        to immediately bootstrap
     */
    public LazyStoreClient(Callable<StoreClient<K, V>> storeClientThunk, boolean instantInit) {
        this.storeClientThunk = storeClientThunk;

        if(instantInit) {
            try {
                storeClient = initStoreClient();
            } catch(Exception e) {
                storeClient = null;
                e.printStackTrace();
                logger.info("Could not bootstrap right away. Trying on the next call ... ");
            }
        }
    }

    public synchronized StoreClient<K, V> getStoreClient() {
        if(storeClient == null)
            storeClient = initStoreClient();

        return storeClient;
    }

    protected StoreClient<K, V> initStoreClient() {
        try {
            return storeClientThunk.call();
        } catch(VoldemortException ve) {
            throw ve;
        } catch(Exception e) {
            // Callable's type signature includes checked exceptions
            throw new VoldemortException("Unexpected exception during initialization", e);
        }
    }

    @Override
    public V getValue(K key, SRD srd) {
        return getStoreClient().getValue(key, srd);
    }

    @Override
    public V getValue(K key, V defaultValue, SRD srd) {
        return getStoreClient().getValue(key, defaultValue, srd);
    }

    @Override
    public Versioned<V> get(K key, SRD srd) {
        return getStoreClient().get(key, srd);
    }

    @Override
    public Versioned<V> get(K key, Object transforms, SRD srd) {
        return getStoreClient().get(key, transforms, srd);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, SRD srd) {
        return getStoreClient().getAll(keys, srd);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms, SRD srd) {
        return getStoreClient().getAll(keys, transforms, srd);
    }

    @Override
    public Versioned<V> get(K key, Versioned<V> defaultValue, SRD srd) {
        return getStoreClient().get(key, defaultValue, srd);
    }

    @Override
    public Version put(K key, V value, SRD srd) {
        return getStoreClient().put(key, value, srd);
    }

    @Override
    public Version put(K key, V value, Object transforms, SRD srd) {
        return getStoreClient().put(key, value, transforms, srd);
    }

    @Override
    public Version put(K key, Versioned<V> versioned, SRD srd) throws ObsoleteVersionException {
        return getStoreClient().put(key, versioned, srd);
    }

    @Override
    public boolean putIfNotObsolete(K key, Versioned<V> versioned, SRD srd) {
        return getStoreClient().putIfNotObsolete(key, versioned, srd);
    }

    @Override
    public boolean applyUpdate(UpdateAction<K, V> action, SRD srd) {
        return getStoreClient().applyUpdate(action, srd);
    }

    @Override
    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries, SRD srd) {
        return getStoreClient().applyUpdate(action, maxTries, srd);
    }

    @Override
    public boolean delete(K key, SRD srd) {
        return getStoreClient().delete(key, srd);
    }

    @Override
    public boolean delete(K key, Version version, SRD srd) {
        return getStoreClient().delete(key, version, srd);
    }

    @Override
    public List<Node> getResponsibleNodes(K key) {
        return getStoreClient().getResponsibleNodes(key);
    }

    @Override
    public Map<ByteArray, Boolean> unlockKeys(Iterable<ByteArray> keys, SRD srd) {
        return getStoreClient().unlockKeys(keys, srd);
    }
}
