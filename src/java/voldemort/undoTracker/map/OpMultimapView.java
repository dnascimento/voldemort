/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import voldemort.utils.ByteArray;

/**
 * Provides a view of OpMultimap.
 * This view is not synchronized
 * 
 * @author darionascimento
 */
public class OpMultimapView {

    private final HashMap<ByteArray, OpMultimapEntry> map;

    public OpMultimapView(HashMap<ByteArray, OpMultimapEntry> map) {
        this.map = map;
    }

    /**
     * Return a key iterator.
     * 
     * @return
     */
    public Set<ByteArray> keySet() {
        return map.keySet();
    }

    public ArrayList<Op> get(ByteArray key) {
        OpMultimapEntry e = map.get(key);
        if(e == null)
            return null;
        return e.getAll();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

}
