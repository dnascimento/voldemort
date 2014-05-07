/**
 * @author: Dario Nascimento
 */
package voldemort.store.contained;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.store.AbstractStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.clientSide.ClientSideTracker;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A store that transforms requests to a Store<ByteArray,byte[], byte[]> to a
 * Store<ByteArray,byte[],byte[]> (identity transformation from black-box
 * perspective).
 * However, the new byte[] is a protobuff message container which attach therud
 * to user data to store it.
 * 
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * @param <T> The type of transform
 */
public class ContainingStore extends AbstractStore<ByteArray, byte[], byte[]> {

    private ClientSideTracker tracker = new ClientSideTracker();

    private final Store<ByteArray, byte[], byte[]> store;
    private final Serializer<byte[]> keySerializer;
    private final ContainingSerializer valueSerializer;
    private final Serializer<byte[]> transformsSerializer;

    public ContainingStore(Store<ByteArray, byte[], byte[]> store) {
        super(store.getName());
        this.store = Utils.notNull(store);
        this.keySerializer = new IdentitySerializer();
        this.valueSerializer = new ContainingSerializer();
        this.transformsSerializer = new IdentitySerializer();
    }

    public static <K1, V1, T1> ContainingStore wrap(Store<ByteArray, byte[], byte[]> s) {
        return new ContainingStore(s);
    }

    @Override
    public boolean delete(ByteArray key, Version version, RUD rud) throws VoldemortException {
        System.out.println("Client Delete: " + rud);
        return store.delete(keyToBytes(key), version, rud);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms, RUD rud)
            throws VoldemortException {
        // Invoke
        List<Versioned<byte[]>> found = store.get(keyToBytes(key),
                                                  (transformsSerializer != null && transforms != null) ? transformsSerializer.toBytes(transforms)
                                                                                                      : null,
                                                  rud);
        // Retrieve
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(found.size());
        for(Versioned<byte[]> versioned: found) {
            Pair<RUD, byte[]> pair = valueSerializer.unpack(versioned.getValue());
            // Here I track the request
            if(pair.getFirst() != null) {
                tracker.trackGet(rud, pair.getFirst());
            }
            results.add(new Versioned<byte[]>(pair.getSecond(), versioned.getVersion()));
        }
        return results;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms,
                                                          RUD rud) throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, ByteArray> byteKeyToKey = keysToBytes(keys);
        Map<ByteArray, List<Versioned<byte[]>>> storeResult = store.getAll(byteKeyToKey.keySet(),
                                                                           transformsToBytes(transforms),
                                                                           rud);
        Map<ByteArray, List<Versioned<byte[]>>> result = Maps.newHashMapWithExpectedSize(storeResult.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: storeResult.entrySet()) {
            List<Versioned<byte[]>> values = Lists.newArrayListWithExpectedSize(mapEntry.getValue()
                                                                                        .size());
            for(Versioned<byte[]> versioned: mapEntry.getValue())
                values.add(new Versioned<byte[]>(valueSerializer.pack(versioned.getValue(), rud),
                                                 versioned.getVersion()));

            result.put(byteKeyToKey.get(mapEntry.getKey()), values);
        }
        return result;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms, RUD rud)
            throws VoldemortException {
        store.put(keyToBytes(key),
                  new Versioned<byte[]>(valueSerializer.pack(value.getValue(), rud),
                                        value.getVersion()),
                  transformToBytes(transforms),
                  rud);
    }

    @Override
    public List<Version> getVersions(ByteArray key, RUD rud) {
        return store.getVersions(keyToBytes(key), rud);
    }

    // ///////////////////// AUX ///////////////////////

    @Override
    public void close() {
        store.close();
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case KEY_SERIALIZER:
                return this.keySerializer;
            case VALUE_SERIALIZER:
                return this.valueSerializer;
            default:
                return store.getCapability(capability);
        }
    }

    private ByteArray keyToBytes(ByteArray key) {
        return key;
    }

    private Map<ByteArray, ByteArray> keysToBytes(Iterable<ByteArray> keys) {
        Map<ByteArray, ByteArray> result = StoreUtils.newEmptyHashMap(keys);
        for(ByteArray key: keys)
            result.put(keyToBytes(key), key);
        return result;
    }

    private byte[] transformToBytes(byte[] transform) {
        return transform;
    }

    private Map<ByteArray, byte[]> transformsToBytes(Map<ByteArray, byte[]> transforms) {
        if(transforms == null)
            return null;
        Map<ByteArray, byte[]> result = Maps.newHashMap();
        for(Map.Entry<ByteArray, byte[]> transform: transforms.entrySet()) {
            result.put(keyToBytes(transform.getKey()), transformToBytes(transform.getValue()));
        }
        return result;
    }

    public Serializer<byte[]> getKeySerializer() {
        return keySerializer;
    }

    public ContainingSerializer getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public Map<ByteArray, Boolean> unlockKeys(Iterable<ByteArray> keys, RUD rud) {
        return store.unlockKeys(keys, rud);
    }
}
