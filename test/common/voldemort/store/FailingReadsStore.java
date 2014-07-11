package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FailingReadsStore<K, V, T> extends AbstractStore<K, V, T> {

    private final InMemoryStorageEngine<K, V, T> engine;

    public FailingReadsStore(String name) {
        super(name);
        this.engine = new InMemoryStorageEngine<K, V, T>(name);
    }

    @Override
    public boolean delete(K key, Version version, RUD rud) throws VoldemortException {
        return engine.delete(key, version, rud);
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms, RUD rud) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public java.util.List<Version> getVersions(K key, RUD rud) {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms, RUD rud)
            throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms, RUD rud) throws VoldemortException {
        engine.put(key, value, transforms, rud);
    }

    @Override
    public Map<ByteArray, Boolean> unlockKeys(Iterable<ByteArray> keys, RUD rud) {
        throw new VoldemortException("Operation failed");
    }
}
