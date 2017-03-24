package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.IdMappingFunction;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    private final ExecutorService threadPool;
    private int labelId;
    private int relationId;
    private int nodeCount;

    public HeavyGraphFactory(
            GraphDatabaseAPI api,
            String label,
            String relation,
            String property,
            final ExecutorService threadPool) {
        super(api, label, relation, property);
        this.threadPool = threadPool;
        withReadOps(readOp -> {
            labelId = readOp.labelGetForName(label);
            relationId = readOp.relationshipTypeGetForName(relation);
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
        });
    }

    @Override
    public Graph build() {
        final IdMap idMap = new IdMap(nodeCount);
        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);

        final WeightMapping weightMapping = relationId == NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(0)
                : new WeightMapping(nodeCount);

        if (threadPool == null) {
            final IdMappingFunction mapOrGet = idMap::mapOrGet;
            withReadOps(readOp -> {
                try (Cursor<NodeItem> cursor = labelId == StatementConstants.NO_SUCH_LABEL
                        ? readOp.nodeCursorGetAll()
                        : readOp.nodeCursorGetForLabel(labelId)) {
                    while (cursor.next()) {
                        final NodeItem node = cursor.get();
                        readNode(node, mapOrGet, 0, matrix);
                    }
                }
            });

        } else {
            final int threads = parallelism(threadPool);
            final int batchSize = nodeCount / threads;

            final List<ImportTask> tasks = new ArrayList<>(threads + 1);
            withReadOps(readOp -> {
                final PrimitiveLongIterator nodeIds = labelId == StatementConstants.NO_SUCH_LABEL
                        ? readOp.nodesGetAll()
                        : readOp.nodesGetForLabel(labelId);
                for (int i = 0; i <= threads; i++) {
                    final ImportTask importTask = new ImportTask(
                            batchSize,
                            idMap,
                            nodeIds);
                    if (importTask.nodeCount > 0) {
                        tasks.add(importTask);
                    }
                }
            });
            run(tasks, threadPool);
            for (ImportTask task : tasks) {
                matrix.addMatrix(task.matrix, task.nodeOffset, task.nodeCount);
            }
        }

        return new HeavyGraph(idMap, matrix, weightMapping);
    }

    private static void readNode(
            NodeItem node,
            IdMappingFunction idMap,
            int idOffset,
            AdjacencyMatrix matrix) {
        final long originalNodeId = node.id();
        final int nodeId = idMap.mapId(originalNodeId) - idOffset;
        final int outDegree = node.degree(Direction.OUTGOING);
        final int inDegree = node.degree(Direction.INCOMING);

        matrix.armOut(nodeId, outDegree);
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.OUTGOING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long endNode = rel.endNode();
                final int targetNodeId = idMap.mapId(endNode);
                final long relationId = rel.id();
                //                try (Cursor<PropertyItem> weights = rel.property(weightPropId)) {
                //                    while (weights.next()) {
                //                        weightMapping.add(relationId, weights.get().value());
                //                    }
                //                }
                matrix.addOutgoing(nodeId, targetNodeId, relationId);
            }
        }
        matrix.armIn(nodeId, inDegree);
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.INCOMING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long startNode = rel.startNode();
                final int targetNodeId = idMap.mapId(startNode);
                final long relationId = rel.id();
                matrix.addIncoming(targetNodeId, nodeId, relationId);
            }
        }
    }

    private static int parallelism(final ExecutorService executorService) {
        final int processors = Runtime.getRuntime().availableProcessors();
        if (executorService instanceof ThreadPoolExecutor) {
            return Math.min(
                    ((ThreadPoolExecutor) executorService).getMaximumPoolSize(),
                    2 * processors);
        }
        if (executorService instanceof ForkJoinPool) {
            return ((ForkJoinPool) executorService).getParallelism();
        }
        return processors;
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
        private final IdMappingFunction idMapper;
        private final long[] nodeIds;
        private final int nodeOffset;
        private final int nodeCount;

        ImportTask(int batchSize, IdMap idMap, PrimitiveLongIterator nodes) {
            this.nodeOffset = idMap.size();
            int i;
            for (i = 0; i < batchSize && nodes.hasNext(); i++) {
                final long nextId = nodes.next();
                idMap.add(nextId);
            }
            this.matrix = new AdjacencyMatrix(batchSize);
            this.idMapper = idMap::get;
            this.nodeIds = idMap.mappedIds();
            this.nodeCount = i;
        }

        @Override
        public void run() {
            withReadOps(this);
        }

        @Override
        public void accept(final ReadOperations readOp) {
            final long[] nodeIds = this.nodeIds;
            final int nodeOffset = this.nodeOffset;
            final int nodeEnd = nodeCount + nodeOffset;
            final IdMappingFunction idMapper = this.idMapper;
            final AdjacencyMatrix matrix = this.matrix;
            for (int i = nodeOffset; i < nodeEnd; i++) {
                try (Cursor<NodeItem> cursor = readOp.nodeCursor(nodeIds[i])) {
                    if (cursor.next()) {
                        HeavyGraphFactory.readNode(
                                cursor.get(),
                                idMapper,
                                nodeOffset,
                                matrix);
                    }
                }
            }
        }
    }
}
