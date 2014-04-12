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

package voldemort.store.contained;

import java.util.List;

import voldemort.server.storage.KeyLockHandle;
import voldemort.store.StorageEngine;
import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * A StorageEngine that handles serialization to bytes, transforming each
 * request to a request to StorageEngine<byte[],byte[], byte[]>
 * 
 * 
 * @param <K> The key type
 * @param <V> The value type
 * @param <T> the transforms type
 */
public class ContainingStorageEngine extends ContainingStore implements
        StorageEngine<ByteArray, byte[], byte[]> {

    private final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    public ContainingStorageEngine(StorageEngine<ByteArray, byte[], byte[]> innerStorageEngine) {
        super(innerStorageEngine);
        this.storageEngine = Utils.notNull(innerStorageEngine);
    }

    public static <K1, V1, T1> ContainingStorageEngine wrap(StorageEngine<ByteArray, byte[], byte[]> s) {
        return new ContainingStorageEngine(s);
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new EntriesIterator(storageEngine.entries());
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return new KeysIterator(storageEngine.keys());
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        return new EntriesIterator(storageEngine.entries(partition));
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        return new KeysIterator(storageEngine.keys(partition));
    }

    @Override
    public void truncate() {
        storageEngine.truncate();
    }

    private class KeysIterator implements ClosableIterator<ByteArray> {

        private final ClosableIterator<ByteArray> iterator;

        public KeysIterator(ClosableIterator<ByteArray> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public ByteArray next() {
            ByteArray key = iterator.next();
            if(key == null)
                return null;
            return key;
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    private class EntriesIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator;

        public EntriesIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            Pair<ByteArray, Versioned<byte[]>> keyAndVal = iterator.next();
            if(keyAndVal == null) {
                return null;
            } else {
                Versioned<byte[]> versioned = keyAndVal.getSecond();
                Pair<RUD, byte[]> pair = getValueSerializer().unpack(versioned.getValue());
                // TODO should I track here too?
                return Pair.create(keyAndVal.getFirst(),
                                   new Versioned<byte[]>(pair.getSecond(), versioned.getVersion()));
            }

        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    @Override
    public boolean isPartitionAware() {
        return storageEngine.isPartitionAware();
    }

    @Override
    public boolean isPartitionScanSupported() {
        return storageEngine.isPartitionScanSupported();
    }

    @Override
    public boolean beginBatchModifications() {
        return false;
    }

    @Override
    public boolean endBatchModifications() {
        return false;
    }

    @Override
    public List<Versioned<byte[]>> multiVersionPut(ByteArray key,
                                                   List<Versioned<byte[]>> values,
                                                   RUD rud) {
        throw new UnsupportedOperationException("multiVersionPut is not supported for "
                                                + this.getClass().getName());
    }

    @Override
    public KeyLockHandle<byte[]> getAndLock(ByteArray key) {
        throw new UnsupportedOperationException("getAndLock is not supported for "
                                                + this.getClass().getName());
    }

    @Override
    public void putAndUnlock(ByteArray key, KeyLockHandle<byte[]> handle) {
        throw new UnsupportedOperationException("putAndUnlock is not supported for "
                                                + this.getClass().getName());
    }

    @Override
    public void releaseLock(KeyLockHandle<byte[]> handle) {
        throw new UnsupportedOperationException("releaseLock is not supported for "
                                                + this.getClass().getName());
    }
}
