package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.concurrent.ExecutorService;

public final class LightGraphFactory extends GraphFactory {

    private IdMap mapping;
    private long[] inOffsets;
    private long[] outOffsets;
    private IntArray adjacency;
    private WeightMapping weights;
    private long adjacencyIdx;
    protected int nodeCount;
    protected int relationCount;
    protected int weightId;

    public LightGraphFactory(GraphDatabaseAPI api, String label, String relation, String property, ExecutorService executorService) {
        super(api, label, relation, property);
        withReadOps(readOp -> {
            nodeCount = Math.toIntExact(readOp.nodesGetCount());
            relationCount = Math.toIntExact(readOp.relationshipsGetCount());
            weightId = property == null ? StatementConstants.NO_SUCH_PROPERTY_KEY : readOp.propertyKeyGetForName(property);
        });
    }

    @Override
    public Graph build() {

        mapping = new IdMap(nodeCount);
        inOffsets = new long[nodeCount];
        outOffsets = new long[nodeCount];
        adjacency = IntArray.newArray(relationCount + nodeCount * 2L);
        weights = weightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(nodeCount)
                : new WeightMapping(0);

        // index 0 is the default for non-connected nodes (by omission of entries)
        adjacencyIdx = 1L;

        withReadOps(readOp -> {
            try(Cursor<NodeItem> cursor = readOp.nodeCursorGetAll()) {
                while (cursor.next()) {
                    readNode(cursor.get());
                }
            }
        });


        return new LightGraph(
                mapping,
                weights,
                adjacency,
                inOffsets,
                outOffsets
        );
    }

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

                try (Cursor<PropertyItem> weights = rel.property(weightId)) {
                    while (weights.next()) { // TODO if/rm?
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
