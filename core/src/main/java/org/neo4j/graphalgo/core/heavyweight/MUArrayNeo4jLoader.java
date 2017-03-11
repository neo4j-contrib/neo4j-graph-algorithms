package org.neo4j.graphalgo.core.heavyweight;

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

public final class MUArrayNeo4jLoader extends GraphLoader<WeightedGraph> {

    private int weightProp;
    private IdMap mapping;
    private WeightMapping weights;
    private AdjacencyMatrix adjacency;
    private long edgeId;

    @Override
    protected void prepare(
            final long nodeCount,
            final long relsCount,
            final int weightProperty) {
        int nodeCapacity = Math.toIntExact(nodeCount);
        mapping = new IdMap(nodeCapacity);
        adjacency = new AdjacencyMatrix(nodeCapacity);
        weightProp = weightProperty;
        weights = weightProperty == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(nodeCapacity)
                : new WeightMapping(0);
    }

    @Override
    protected void readNode(final NodeItem node) {
        long sourceNodeId = node.id();
        int sourceGraphId = mapping.mapOrGet(sourceNodeId);

        readRelationships(sourceGraphId, node);
    }

    @Override
    protected HeavyGraph build() {
        return new HeavyGraph(mapping, adjacency, weights);
    }

    private void readRelationships(
            int sourceGraphId,
            NodeItem node) {
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.OUTGOING)) {
            while (rels.next()) {
                RelationshipItem rel = rels.get();

                long targetNodeId = rel.otherNode(node.id());
                int targetGraphId = mapping.mapOrGet(targetNodeId);

                try (Cursor<PropertyItem> weights = rel.property(weightProp)) {
                    while (weights.next()) {
                        this.weights.add(edgeId, weights.get().value());
                    }
                }
                adjacency.add(sourceGraphId, targetGraphId, edgeId++);
            }
        }
    }
}
