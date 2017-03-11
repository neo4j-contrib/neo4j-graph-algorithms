package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

public final class LowLevelNeo4jLoader extends GraphLoader<WeightedGraph> {

    private IdMap mapping;
    private long[] inOffsets;
    private long[] outOffsets;
    private IntArray adjacency;
    private WeightMapping weights;
    private long adjacencyIdx;
    private int weightProp;

    @Override
    protected void prepare(
            final long nodeCount,
            final long relsCount,
            final int weightProperty) {

        int intNodeCount = Math.toIntExact(nodeCount);
        mapping = new IdMap(intNodeCount);
        inOffsets = new long[intNodeCount];
        outOffsets = new long[intNodeCount];
        adjacency = IntArray.newArray(relsCount + nodeCount * 2L);
        weightProp = weightProperty;
        weights = weightProperty == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(intNodeCount)
                : new WeightMapping(0);

        // index 0 is the default for non-connected nodes (by omission of entries)
        adjacencyIdx = 1L;
    }

    @Override
    protected void readNode(final NodeItem node) {
        long sourceNodeId = node.id();
        int sourceGraphId = mapping.mapOrGet(sourceNodeId);

        readRelationships(
                sourceGraphId,
                node,
                Direction.OUTGOING,
                outOffsets
        );
        readRelationships(
                sourceGraphId,
                node,
                Direction.INCOMING,
                inOffsets
        );
    }

    @Override
    protected WeightedGraph build() {
        return new LightWeightGraph(
                mapping,
                weights,
                adjacency,
                inOffsets,
                outOffsets
        );
    }

    private void readRelationships(
            int sourceGraphId,
            NodeItem node,
            Direction direction,
            long[] offsets) {
        int relDegree = 0;
        long idx = adjacencyIdx + 1L;
        try (Cursor<RelationshipItem> rels = node.relationships(direction)) {
            while (rels.next()) {
                relDegree++;
                RelationshipItem rel = rels.get();

                long targetNodeId = rel.otherNode(node.id());
                int targetGraphId = mapping.mapOrGet(targetNodeId);

                try (Cursor<PropertyItem> weights = rel.property(weightProp)) {
                    while (weights.next()) {
                        this.weights.add(idx, weights.get().value());
                    }
                }

                adjacency.grow(idx + 1);
                adjacency.set(idx++, targetGraphId);
            }
        }
        if (relDegree > 0) {
            offsets[sourceGraphId] = adjacencyIdx;
            adjacency.set(adjacencyIdx, relDegree);
            adjacencyIdx = idx;
        }
    }
}
