package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);

        final WeightMapping relWeigths = relWeightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.relationDefaultWeight)
                : new WeightMap(nodeCount, setup.relationDefaultWeight);

        final WeightMapping nodeWeights = nodeWeightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.nodeDefaultWeight)
                : new WeightMap(nodeCount, setup.nodeDefaultWeight);

        final WeightMapping nodeProps = nodePropId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(setup.nodeDefaultPropertyValue)
                : new WeightMap(nodeCount, setup.nodeDefaultPropertyValue);

        int threads = (int) Math.ceil(nodeCount / (double) batchSize);

        if (threadPool == null || threads == 1) {
            withReadOps(readOp -> {
                final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                        ? readOp.nodesGetAll()
                        : readOp.nodesGetForLabel(labelId);
                while (nodeIds.hasNext()) {
                    final long nextId = nodeIds.next();
                    idMap.add(nextId);
                }
                idMap.buildMappedIds();

                try (Cursor<NodeItem> cursor = labelId == ReadOperations.ANY_LABEL
                        ? readOp.nodeCursorGetAll()
                        : readOp.nodeCursorGetForLabel(labelId)) {
                    while (cursor.next()) {
                        final NodeItem node = cursor.get();
                        readNode(node,
                                idMap,
                                0,
                                matrix,
                                relWeightId,
                                relWeigths,
                                nodeWeightId,
                                nodeWeights,
                                nodePropId,
                                nodeProps,
                                relationId);
                    }
                }
            });
        } else {
            final List<ImportTask> tasks = new ArrayList<>(threads);
            withReadOps(readOp -> {
                final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                        ? readOp.nodesGetAll()
                        : readOp.nodesGetForLabel(labelId);
                for (int i = 0; i <= threads; i++) {
                    final ImportTask importTask = new ImportTask(
                            batchSize,
                            idMap,
                            nodeIds,
                            relWeigths,
                            nodeWeights,
                            nodeProps,
                            relationId
                    );
                    if (importTask.nodeCount > 0) {
                        tasks.add(importTask);
                    }
                }
            });
            idMap.buildMappedIds();
            run(tasks, threadPool);
            for (ImportTask task : tasks) {
                matrix.addMatrix(task.matrix, task.nodeOffset, task.nodeCount);
            }
        }
        return new HeavyGraph(
                idMap,
                matrix,
                relWeigths,
                nodeWeights,
                nodeProps);
    }

    private static void readNode(
            NodeItem node,
            IdMap idMap,
            int idOffset,
            AdjacencyMatrix matrix,
            int relWeightId,
            WeightMapping relWeights,
            int nodeWeightId,
            WeightMapping nodeWeights,
            int nodePropId,
            WeightMapping nodeProps,
            int... relationType
    ) {
        final long originalNodeId = node.id();
        final int nodeId = idMap.get(originalNodeId) - idOffset;
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
                final long relationId = rel.id();

                try (Cursor<PropertyItem> weights = rel.property(relWeightId)) {
                    if (weights.next()) {
                        relWeights.set(relationId, weights.get().value());
                    }
                }

                matrix.addOutgoing(nodeId, targetNodeId, relationId);
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
                final long relationId = rel.id();
                matrix.addIncoming(targetNodeId, nodeId, relationId);
            }
        }
    }

    private static void run(
            final Collection<? extends Runnable> tasks,
            final ExecutorService threadPool) {
        final List<Future<?>> futures = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            futures.add(threadPool.submit(task));
        }

        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    private final class ImportTask implements Runnable, Consumer<ReadOperations> {
        private final AdjacencyMatrix matrix;
        private final int nodeOffset;
        private final int nodeCount;
        private final IdMap idMap;
        private final WeightMapping relWeights;
        private final WeightMapping nodeWeights;
        private final WeightMapping nodeProps;
        private final int[] relationId;

        ImportTask(
                int batchSize,
                IdMap idMap,
                PrimitiveLongIterator nodes,
                WeightMapping relWeights,
                WeightMapping nodeWeights,
                WeightMapping nodeProps,
                int... relationId) {
            this.idMap = idMap;
            this.nodeOffset = idMap.size();
            this.relWeights = relWeights;
            this.nodeWeights = nodeWeights;
            this.nodeProps = nodeProps;
            this.relationId = relationId;
            int i;
            for (i = 0; i < batchSize && nodes.hasNext(); i++) {
                final long nextId = nodes.next();
                idMap.add(nextId);
            }
            this.matrix = new AdjacencyMatrix(batchSize);
            this.nodeCount = i;
        }

        @Override
        public void run() {
            withReadOps(this);
        }

        @Override
        public void accept(final ReadOperations readOp) {
            final long[] nodeIds = idMap.mappedIds();
            final int nodeOffset = this.nodeOffset;
            final int nodeEnd = nodeCount + nodeOffset;
            for (int i = nodeOffset; i < nodeEnd; i++) {
                try (Cursor<NodeItem> cursor = readOp.nodeCursor(nodeIds[i])) {
                    if (cursor.next()) {
                        HeavyGraphFactory.readNode(
                                cursor.get(),
                                idMap,
                                nodeOffset,
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
        }
    }
}
