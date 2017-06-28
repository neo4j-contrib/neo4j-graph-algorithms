package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.predicates.IntObjectPredicate;

import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class PathsBag {

    public static final int INITIAL_PATH_CAPACITY = 100;

    private final IntObjectMap<IntArrayList> paths;

    public PathsBag() {
        this(10000);
    }

    public PathsBag(int expectedElements) {
        paths = new IntObjectScatterMap<>(expectedElements);
    }


    public void append(int pathId, int nodeId) {
        final IntArrayList path;
        if (!paths.containsKey(pathId)) {
            path = new IntArrayList(INITIAL_PATH_CAPACITY);
            paths.put(pathId, path);
        } else {
            path = paths.get(pathId);
        }
        path.add(nodeId);
    }

    public int size(int pathId) {
        return paths.containsKey(pathId) ? paths.get(pathId).size() : 0;
    }

    public void forEach(int pathId, IntPredicate consumer) {
        if (paths.containsKey(pathId)) {
            paths.get(pathId).forEach((Consumer<? super IntCursor>) c -> consumer.test(c.value));
        }
    }

    public void clear() {
        paths.clear();
    }
}
