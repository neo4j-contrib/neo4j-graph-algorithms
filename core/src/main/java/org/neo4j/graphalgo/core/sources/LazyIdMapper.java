package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.IntLongScatterMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntScatterMap;
import org.neo4j.graphalgo.api.IdMapping;

/**
 * @author mknblch
 */
public class LazyIdMapper implements IdMapping {

    private final LongIntMap forward;
    private final IntLongMap backward;

    private int current = 0;

    public LazyIdMapper() {
        forward = new LongIntScatterMap();
        backward = new IntLongScatterMap();
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        int value = forward.getOrDefault(nodeId, -1);
        if (value != -1) {
            return value;
        }
        forward.put(nodeId, current);
        backward.put(current, nodeId);
        return current++;
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return backward.getOrDefault(nodeId, -1);
    }

    @Override
    public int nodeCount() {
        return forward.size();
    }
}