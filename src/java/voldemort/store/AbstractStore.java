package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.undoTracker.SRD;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public abstract class AbstractStore<K, V, T> implements Store<K, V, T> {

    private final String storeName;

    public AbstractStore(String name) {
        this.storeName = Utils.notNull(name);
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms, SRD srd) throws VoldemortException {
        return null;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms, SRD srd)
            throws VoldemortException {
        return null;
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms, SRD srd) throws VoldemortException {}

    @Override
    public boolean delete(K key, Version version, SRD srd) throws VoldemortException {
        return false;
    }

    @Override
    public String getName() {
        return this.storeName;
    }

    @Override
    public void close() throws VoldemortException {}

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    @Override
    public List<Version> getVersions(K key, SRD srd) {
        return null;
    }

    @Override
    public List<Versioned<V>> get(CompositeVoldemortRequest<K, V> request)
            throws VoldemortException {
        return null;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(CompositeVoldemortRequest<K, V> request)
            throws VoldemortException {
        return null;
    }

    @Override
    public void put(CompositeVoldemortRequest<K, V> request) throws VoldemortException {}

    @Override
    public boolean delete(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        return false;
    }

    @Override
    public Map<ByteArray, Boolean> unlockKeys(Iterable<ByteArray> keys, SRD srd) {
        return null;
    }

}
