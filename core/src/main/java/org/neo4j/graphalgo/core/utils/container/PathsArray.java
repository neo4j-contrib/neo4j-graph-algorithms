package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.util.ArrayUtil;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class PathsArray {

    public static final int INITIAL_PATH_CAPACITY = 100;

    private final int[][] paths;
    private final int offset[];

    public PathsArray(int nodeCount) {
        paths = new int[nodeCount][];
        offset = new int[nodeCount];
    }

    public void append(int pathId, int nodeId) {
        if (null != paths[pathId]) {
            paths[pathId] = ArrayUtil.grow(paths[pathId], offset[pathId] + 1);
        } else {
            paths[pathId] = new int[INITIAL_PATH_CAPACITY];
        }
        paths[pathId][offset[pathId]++] = nodeId;
    }

    public int size(int pathId) {
        return offset[pathId];
    }

    public void forEach(int pathId, IntPredicate consumer) {
        for (int off = offset[pathId] - 1; off >= 0; off--) {
            consumer.test(paths[pathId][off]);
        }
    }

    public void clear() {
        Arrays.fill(offset, 0);
    }
}
