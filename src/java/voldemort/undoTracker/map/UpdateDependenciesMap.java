package voldemort.undoTracker.map;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import undo.proto.ToManagerProto;
import undo.proto.ToManagerProto.TrackEntry;
import undo.proto.ToManagerProto.TrackMsg;

public class UpdateDependenciesMap {

    class Wrapper<T> {

        public T[] array;

        public Wrapper(T[] array) {
            this.array = array;
        }
    }

    public class WrapperLong implements Iterable<Long>, Iterator<Long> {

        private static final int INCREMENT_ARRAY = 20;
        public long[] array;
        int p = 0;
        int it = 0;

        public WrapperLong(long[] array) {
            this.array = array;
        }

        public void add(long entry) {
            if(p == array.length) {
                long[] newArray = new long[array.length + INCREMENT_ARRAY];
                for(int i = 0; i < array.length; i++) {
                    newArray[i] = array[i];
                }
                array = newArray;
            }
            array[p++] = entry;
        }

        @Override
        public Iterator<Long> iterator() {
            it = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return (it != array.length) && (array[it] != 0);
        }

        @Override
        public Long next() {
            return array[it++];
        }

        @Override
        public void remove() {}

    }

    private static int AVG_NUMBER_KEYS = 500;

    private static final int AVG_NUMBER_DEPENDENCIES = 10;

    LinkedList<Wrapper<Op>> dependencies = new LinkedList<Wrapper<Op>>();
    int pointer = 1;
    Op[] currentArray;

    /**
     * Invoked before to allocate a new array with size and the pointer
     * 
     * @param lastWrite
     * 
     * @param remaining
     */
    public void prepareNewBatch(int size, Op lastWrite) {
        currentArray = new Op[size + 1];
        currentArray[0] = lastWrite;
        pointer = 1;
        dependencies.add(new Wrapper<Op>(currentArray));
    }

    // adding the operations
    public void putNew(Op op) {
        currentArray[pointer++] = op;
    }

    public TrackMsg convertToList() {
        HashMap<Long, WrapperLong> map = createDependencyMap();
        // convert to protobuff
        return convertToList(map);
    }

    public HashMap<Long, WrapperLong> createDependencyMap() {
        HashMap<Long, WrapperLong> map = new HashMap<Long, WrapperLong>(AVG_NUMBER_KEYS);

        // Generate dependencies per RID, ignore 69L RID
        for(Wrapper<Op> wrapper: dependencies) {
            Op[] array = wrapper.array;
            long lastWrite = array[0].rid;

            for(int i = 1; i < array.length; i++) {
                Op op = array[i];
                if(op.rid == 69L) {
                    continue;
                }
                if(op.type == Op.OpType.Get) {
                    WrapperLong list = map.get(op.rid);
                    if(list == null) {
                        long[] dependencies = new long[AVG_NUMBER_DEPENDENCIES];
                        list = new WrapperLong(dependencies);
                        map.put(op.rid, list);
                    }
                    list.add(lastWrite);
                } else {
                    lastWrite = op.rid;
                }
            }
        }

        if(map.size() == 0) {
            return null;
        }
        AVG_NUMBER_KEYS = AVG_NUMBER_KEYS + ((map.size() - AVG_NUMBER_KEYS) / 2);
        return map;
    }

    /**
     * For each key, get the list and convert to tracklist
     * 
     * @return
     */
    private ToManagerProto.TrackMsg convertToList(HashMap<Long, WrapperLong> map) {
        TrackMsg.Builder listBuilder = TrackMsg.newBuilder();
        for(Long key: map.keySet()) {
            TrackEntry.Builder entry = TrackEntry.newBuilder();
            entry.setRid(key);
            entry.addAllDependency(map.get(key));
            listBuilder.addEntry(entry.build());
        }
        return listBuilder.build();
    }

    public long count() {
        long counter = 0;
        HashMap<Long, WrapperLong> map = createDependencyMap();
        for(Long key: map.keySet()) {
            counter += map.get(key).array.length;
        }
        return counter;
    }

}
