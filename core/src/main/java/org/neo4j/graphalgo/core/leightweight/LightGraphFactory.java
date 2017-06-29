package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.Kernel;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.IdCombiner;
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
    private IntArray.BulkAdder inAdder;
    private IntArray.BulkAdder outAdder;
    private WeightMapping weights;
    private long inAdjacencyIdx;
    private long outAdjacencyIdx;
    protected int nodeCount;
    private int labelId;
    private int relationId;
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
                    relationId = relId;
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
        boolean loadIncoming = setup.loadIncoming;
        boolean loadOutgoing = setup.loadOutgoing;

        mapping = new IdMap(nodeCount);
        // we allocate one more offset in order to avoid having to
        // check for the last element during degree access
        if (loadIncoming) {
            inOffsets = new long[nodeCount + 1];
            inAdjacency = IntArray.newArray(nodeCount);
            inAdder = inAdjacency.bulkAdder();
        }
        if (loadOutgoing) {
            outOffsets = new long[nodeCount + 1];
            outOffsets[nodeCount] = nodeCount;
            outAdjacency = IntArray.newArray(nodeCount);
            outAdder = outAdjacency.bulkAdder();
        }
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
                mapping.add(nodeIds.next());
            }
            mapping.buildMappedIds();

            try (Cursor<Kernel.NodeItem> cursor = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodeCursorGetAll()
                    : readOp.nodeCursorGetForLabel(labelId)) {
                while (cursor.next()) {
                    readNode(cursor.get(), loadIncoming, loadOutgoing);
                }
            }
        });

        if (inOffsets != null) {
            inOffsets[nodeCount] = inAdjacencyIdx;
        }
        if (outOffsets != null) {
            outOffsets[nodeCount] = outAdjacencyIdx;
        }

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
            Kernel.NodeItem node,
            boolean loadIncoming,
            boolean loadOutgoing) {
        long sourceNodeId = node.id();
        int sourceGraphId = mapping.get(sourceNodeId);

        if (loadOutgoing) {
            outAdjacencyIdx = readRelationships(
                    sourceGraphId,
                    node,
                    Direction.OUTGOING,
                    RawValues.OUTGOING,
                    outOffsets,
                    outAdjacency,
                    outAdder,
                    outAdjacencyIdx
            );
        }
        if (loadIncoming) {
            inAdjacencyIdx = readRelationships(
                    sourceGraphId,
                    node,
                    Direction.INCOMING,
                    RawValues.INCOMING,
                    inOffsets,
                    inAdjacency,
                    inAdder,
                    inAdjacencyIdx
            );
        }
    }

    private long readRelationships(
            int sourceGraphId,
            Kernel.NodeItem node,
            Direction direction,
            IdCombiner idCombiner,
            long[] offsets,
            IntArray adjacency,
            IntArray.BulkAdder bulkAdder,
            long adjacencyIdx) {

        offsets[sourceGraphId] = adjacencyIdx;
        int degree = relationId == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
                ? node.degree(direction)
                : node.degree(direction, relationId);

        if (degree > 0) {
            adjacency.bulkAdder(adjacencyIdx, degree, bulkAdder);
            try (Cursor<Kernel.RelationshipItem> rels = relationId == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
                    ? node.relationships(direction)
                    : node.relationships(direction, relationId)) {
                while (rels.next()) {
                    Kernel.RelationshipItem rel = rels.get();

                    long targetNodeId = rel.otherNode(node.id());
                    int targetGraphId = mapping.get(targetNodeId);
                    if (targetGraphId == -1) {
                        continue;
                    }
                    if (weightId != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                        try (Cursor<PropertyItem> weights = rel.property(weightId)) {
                            if (weights.next()) {
                                long relId = idCombiner.apply(
                                        sourceGraphId,
                                        targetGraphId);
                                this.weights.set(relId, weights.get().value());
                            }
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
