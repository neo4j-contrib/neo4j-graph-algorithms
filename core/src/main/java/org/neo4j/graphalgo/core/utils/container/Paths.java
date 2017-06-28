package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class Paths {

    public static final int INITIAL_PATH_CAPACITY = 100;

    private final IntObjectMap<Path> paths;

    public Paths() {
        this(10000);
    }

    public Paths(int expectedElements) {
        paths = new IntObjectScatterMap<>(expectedElements);
    }


    public void append(int pathId, int nodeId) {
        final Path path;
        if (!paths.containsKey(pathId)) {
            path = new Path(INITIAL_PATH_CAPACITY);
            paths.put(pathId, path);
        } else {
            path = paths.get(pathId);
        }
        path.append(nodeId);
    }

    public int size(int pathId) {
        return paths.containsKey(pathId) ? paths.get(pathId).size() : 0;
    }

    public void forEach(int pathId, IntPredicate consumer) {
        if (paths.containsKey(pathId)) {
            paths.get(pathId).forEach(consumer);
        }
    }

    public void clear() {
        paths.forEach((Consumer<IntObjectCursor<Path>>) p -> p.value.clear());
    }
}
