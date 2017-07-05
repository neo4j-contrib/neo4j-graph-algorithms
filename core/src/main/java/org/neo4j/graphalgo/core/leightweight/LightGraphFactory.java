package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

public final class LightGraphFactory extends GraphFactory {

    private IdMap mapping;
    private long[] inOffsets;
    private long[] outOffsets;
    private IntArray inAdjacency;
    private IntArray outAdjacency;
    private WeightMapping weights;
    private long inAdjacencyIdx;
    private long outAdjacencyIdx;
    protected int nodeCount;
    private int labelId;
    private int[] relationId;
    private int weightId;

    public LightGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
        withReadOps(readOp -> {
            labelId = setup.loadAnyLabel()
                    ? ReadOperations.ANY_LABEL
                    : readOp.labelGetForName(setup.startLabel);
            if (!setup.loadAnyRelationshipType()) {
                int relId = readOp.relationshipTypeGetForName(setup.relationshipType);
                if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                    relationId = new int[]{relId};
                }
            }
            weightId = setup.loadDefaultRelationshipWeight()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
        });
    }

    @Override
    public Graph build() {

        mapping = new IdMap(nodeCount);
        // we allocate one more offset and set it to the nodeCount in order
        // to avoid having to check for the last element during degree access
        inOffsets = new long[nodeCount + 1];
        outOffsets = new long[nodeCount + 1];
        inOffsets[nodeCount] = nodeCount;
        outOffsets[nodeCount] = nodeCount;
        inAdjacency = IntArray.newArray(nodeCount);
        outAdjacency = IntArray.newArray(nodeCount);
        weights = weightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.relationDefaultWeight)
                : new WeightMap(nodeCount, setup.relationDefaultWeight);

        inAdjacencyIdx = 0L;
        outAdjacencyIdx = 0L;

        withReadOps(readOp -> {
            final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodesGetAll()
                    : readOp.nodesGetForLabel(labelId);
            while (nodeIds.hasNext()) {
                final long nextId = nodeIds.next();
                mapping.add(nextId);
            }
            mapping.buildMappedIds();

            IntArray.BulkAdder inBulkAdder = inAdjacency.bulkAdder();
            IntArray.BulkAdder outBulkAdder = outAdjacency.bulkAdder();

            try (Cursor<NodeItem> cursor = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodeCursorGetAll()
                    : readOp.nodeCursorGetForLabel(labelId)) {
                while (cursor.next()) {
                    readNode(cursor.get(), inBulkAdder, outBulkAdder);
                }
            }
        });

        return new LightGraph(
                mapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }

    private void readNode(
            NodeItem node,
            IntArray.BulkAdder inBulkAdder,
            IntArray.BulkAdder outBulkAdder) {
        long sourceNodeId = node.id();
        int sourceGraphId = mapping.get(sourceNodeId);

        outAdjacencyIdx = readRelationships(
                sourceGraphId,
                node,
                Direction.OUTGOING,
                outOffsets,
                outAdjacency,
                outBulkAdder,
                outAdjacencyIdx
        );
        inAdjacencyIdx = readRelationships(
                sourceGraphId,
                node,
                Direction.INCOMING,
                inOffsets,
                inAdjacency,
                inBulkAdder,
                inAdjacencyIdx
        );
    }

    private long readRelationships(
            int sourceGraphId,
            NodeItem node,
            Direction direction,
            long[] offsets,
            IntArray adjacency,
            IntArray.BulkAdder bulkAdder,
            long adjacencyIdx) {

        offsets[sourceGraphId] = adjacencyIdx;
        int degree = relationId == null
                ? node.degree(direction)
                : node.degree(direction, relationId[0]);

        if (degree > 0) {
            adjacency.bulkAdder(adjacencyIdx, degree, bulkAdder);
            try (Cursor<RelationshipItem> rels = relationId == null
                    ? node.relationships(direction)
                    : node.relationships(direction, relationId)) {
                while (rels.next()) {
                    RelationshipItem rel = rels.get();

                    long targetNodeId = rel.otherNode(node.id());
                    int targetGraphId = mapping.get(targetNodeId);
                    if (targetGraphId == -1) {
                        continue;
                    }

                    try (Cursor<PropertyItem> weights = rel.property(weightId)) {
                        if (weights.next()) {
                            long relId = RawValues.combineIntInt(
                                    sourceGraphId,
                                    targetGraphId);
                            this.weights.set(relId, weights.get().value());
                        }
                    }

                    bulkAdder.add(targetGraphId);
                    adjacencyIdx++;
                }
            }
        }
        return adjacencyIdx;
    }
}
