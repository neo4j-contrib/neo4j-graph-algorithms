package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.IntLongScatterMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntScatterMap;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class LazyIdMapper implements IdMapping {

    private final LongIntMap forward;
    private final IntLongMap backward;
    private final int nodeCount;

    private int current = 0;

    /**
     * initialize without predefined size so that
     * {@link LazyIdMapper#nodeCount()} returns the
     * current number of objects
     */
    public LazyIdMapper() {
        forward = new LongIntScatterMap();
        backward = new IntLongScatterMap();
        this.nodeCount = -1;
    }

    /**
     * initialize the id-map with a predefined node count.
     *
     * @param nodeCount the number of nodes to map
     */
    public LazyIdMapper(int nodeCount) {
        forward = new LongIntScatterMap(nodeCount);
        backward = new IntLongScatterMap(nodeCount);
        this.nodeCount = nodeCount;
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
        return nodeCount == -1 ? forward.size() : nodeCount;
    }

    public static LazyIdMapperImporter importer(GraphDatabaseAPI api) {
        return new LazyIdMapperImporter(api);
    }

    public static class LazyIdMapperImporter extends Importer<LazyIdMapper, LazyIdMapperImporter> {

        public LazyIdMapperImporter(GraphDatabaseAPI api) {
            super(api);
        }

        @Override
        protected LazyIdMapperImporter me() {
            return this;
        }

        @Override
        protected LazyIdMapper buildT() {
            return new LazyIdMapper(nodeCount);
        }
    }
}