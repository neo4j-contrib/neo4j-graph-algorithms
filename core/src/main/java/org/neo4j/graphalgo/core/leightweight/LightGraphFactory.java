package org.neo4j.graphalgo.core.leightweight;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
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
    private IntArray adjacency;
    private WeightMapping weights;
    private LongLongMap relationIdMapping;
    private long adjacencyIdx;
    protected int nodeCount;
    private int relationCount;
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
            relationCount = Math.toIntExact(relationId == null
                    ? readOp.countsForRelationship(labelId, ReadOperations.ANY_RELATIONSHIP_TYPE, ReadOperations.ANY_LABEL)
                    : readOp.countsForRelationship(labelId, relationId[0], ReadOperations.ANY_LABEL));
        });
    }

    @Override
    public Graph build() {

        mapping = new IdMap(nodeCount);
        inOffsets = new long[nodeCount];
        outOffsets = new long[nodeCount];
        relationIdMapping = new LongLongHashMap(
                (int) Math.ceil(relationCount / 0.99),
                0.99);
        adjacency = IntArray.newArray(relationCount + nodeCount * 2L);
        weights = weightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.relationDefaultWeight)
                : new WeightMap(nodeCount, setup.relationDefaultWeight);

        // index 0 is the default for non-connected nodes (by omission of entries)
        adjacencyIdx = 1L;

        withReadOps(readOp -> {
            final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodesGetAll()
                    : readOp.nodesGetForLabel(labelId);
            while (nodeIds.hasNext()) {
                final long nextId = nodeIds.next();
                mapping.add(nextId);
            }
            mapping.buildMappedIds();

            try (Cursor<NodeItem> cursor = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodeCursorGetAll()
                    : readOp.nodeCursorGetForLabel(labelId)) {
                while (cursor.next()) {
                    readNode(cursor.get());
                }
            }
        });
        mapping.buildMappedIds();

        return new LightGraph(
                mapping,
                weights,
                relationIdMapping,
                adjacency,
                inOffsets,
                outOffsets
        );
    }

    private void readNode(final NodeItem node) {
        long sourceNodeId = node.id();
        int sourceGraphId = mapping.get(sourceNodeId);

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

                relDegree++;
                relationIdMapping.put(idx, rel.id());

                try (Cursor<PropertyItem> weights = rel.property(weightId)) {
                    if (weights.next()) {
                        this.weights.set(idx, weights.get().value());
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
