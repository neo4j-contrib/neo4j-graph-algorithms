package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    private static final int BATCH_SIZE = 100_000;

    private final ExecutorService threadPool;
    private int relWeightId;
    private int nodeWeightId;
    private int nodePropId;
    private int labelId;
    private int[] relationId;
    private int nodeCount;

    public HeavyGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
        this.threadPool = setup.executor;
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
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
            relWeightId = setup.loadDefaultRelationshipWeight()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
            nodeWeightId = setup.loadDefaultNodeWeight()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.nodeWeightPropertyName);
            nodePropId = setup.loadDefaultNodeProperty()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.nodePropertyName);
        });
    }

    @Override
    public Graph build() {
        return build(BATCH_SIZE);
    }

    /* test-private */ Graph build(int batchSize) {
        final IdMap idMap = new IdMap(nodeCount);

        final WeightMapping relWeigths = relWeightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.relationDefaultWeight)
                : new WeightMap(nodeCount, setup.relationDefaultWeight);

        final WeightMapping nodeWeights = nodeWeightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.nodeDefaultWeight)
                : new WeightMap(nodeCount, setup.nodeDefaultWeight);

        final WeightMapping nodeProps = nodePropId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.nodeDefaultPropertyValue)
                : new WeightMap(nodeCount, setup.nodeDefaultPropertyValue);

        withReadOps(read -> {
            final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                    ? read.nodesGetAll()
                    : read.nodesGetForLabel(labelId);
            while (nodeIds.hasNext()) {
                final long nextId = nodeIds.next();
                idMap.add(nextId);
            }
            idMap.buildMappedIds();
        });

        Collection<ImportTask> tasks = ParallelUtil.readParallel(
                batchSize,
                idMap,
                (offset, nodeIds) -> new ImportTask(
                        batchSize,
                        offset,
                        idMap,
                        nodeIds,
                        relWeigths,
                        nodeWeights,
                        nodeProps,
                        relationId
                ),
                threadPool);

        return new HeavyGraph(
                idMap,
                buildAdjacencyMatrix(tasks),
                relWeigths,
                nodeWeights,
                nodeProps);
    }

    private AdjacencyMatrix buildAdjacencyMatrix(Collection<ImportTask> tasks) {
        if (tasks.size() == 1) {
            ImportTask task = tasks.iterator().next();
            if (task.matrix.capacity() == task.nodeCount) {
                return task.matrix;
            }
        }
        AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);
        for (ImportTask task : tasks) {
            matrix.addMatrix(task.matrix, task.nodeOffset, task.nodeCount);
        }
        return matrix;
    }

    private static void readNode(
            NodeItem node,
            int nodeId,
            IdMap idMap,
            AdjacencyMatrix matrix,
            int relWeightId,
            WeightMapping relWeights,
            int nodeWeightId,
            WeightMapping nodeWeights,
            int nodePropId,
            WeightMapping nodeProps,
            int... relationType) {
        final int outDegree;
        final int inDegree;
        final Cursor<RelationshipItem> outCursor;
        final Cursor<RelationshipItem> inCursor;
        if (relationType == null) {
            outDegree = node.degree(Direction.OUTGOING);
            inDegree = node.degree(Direction.INCOMING);
            outCursor = node.relationships(Direction.OUTGOING);
            inCursor = node.relationships(Direction.INCOMING);
        } else {
            outDegree = node.degree(Direction.OUTGOING, relationType[0]);
            inDegree = node.degree(Direction.INCOMING, relationType[0]);
            outCursor = node.relationships(Direction.OUTGOING, relationType);
            inCursor = node.relationships(Direction.INCOMING, relationType);
        }
        try (Cursor<PropertyItem> weights = node.property(nodeWeightId)) {
            if (weights.next()) {
                nodeWeights.set(nodeId, weights.get().value());
            }
        }
        try (Cursor<PropertyItem> props = node.property(nodePropId)) {
            if (props.next()) {
                nodeProps.set(nodeId, props.get().value());
            }
        }

        matrix.armOut(nodeId, outDegree);
        try (Cursor<RelationshipItem> rels = outCursor) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long endNode = rel.endNode();
                final int targetNodeId = idMap.get(endNode);
                if (targetNodeId == -1) {
                    continue;
                }
                final long relationId = RawValues.combineIntInt(nodeId, targetNodeId);

                try (Cursor<PropertyItem> weights = rel.property(relWeightId)) {
                    if (weights.next()) {
                        relWeights.set(relationId, weights.get().value());
                    }
                }

                matrix.addOutgoing(nodeId, targetNodeId);
            }
        }
        matrix.armIn(nodeId, inDegree);
        try (Cursor<RelationshipItem> rels = inCursor) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long startNode = rel.startNode();
                final int targetNodeId = idMap.get(startNode);
                if (targetNodeId == -1) {
                    continue;
                }
                matrix.addIncoming(targetNodeId, nodeId);
            }
        }
    }

    private final class ImportTask implements Runnable, Consumer<ReadOperations> {
        private final AdjacencyMatrix matrix;
        private final int nodeOffset;
        private int nodeCount;
        private final IdMap idMap;
        private final PrimitiveIntIterable nodes;
        private final WeightMapping relWeights;
        private final WeightMapping nodeWeights;
        private final WeightMapping nodeProps;
        private final int[] relationId;

        ImportTask(
                int batchSize,
                int nodeOffset,
                IdMap idMap,
                PrimitiveIntIterable nodes,
                WeightMapping relWeights,
                WeightMapping nodeWeights,
                WeightMapping nodeProps,
                int... relationId) {
            int nodeSize = Math.min(batchSize, idMap.size() - nodeOffset);
            this.nodeOffset = nodeOffset;
            this.idMap = idMap;
            this.nodes = nodes;
            this.relWeights = relWeights;
            this.nodeWeights = nodeWeights;
            this.nodeProps = nodeProps;
            this.relationId = relationId;
            this.matrix = new AdjacencyMatrix(nodeSize);
            this.nodeCount = 0;
        }

        @Override
        public void run() {
            withReadOps(this);
        }

        @Override
        public void accept(final ReadOperations readOp) {
            int nodeOffset = this.nodeOffset;
            int nodeCount = 0;
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                int nodeId = iterator.next();
                try (Cursor<NodeItem> cursor = readOp.nodeCursor(idMap.toOriginalNodeId(nodeId))) {
                    if (cursor.next()) {
                        nodeCount++;
                        HeavyGraphFactory.readNode(
                                cursor.get(),
                                nodeId - nodeOffset,
                                idMap,
                                matrix,
                                relWeightId,
                                relWeights,
                                nodeWeightId,
                                nodeWeights,
                                nodePropId,
                                nodeProps,
                                relationId);
                    }
                }
            }
            this.nodeCount = nodeCount;
        }
    }
}
