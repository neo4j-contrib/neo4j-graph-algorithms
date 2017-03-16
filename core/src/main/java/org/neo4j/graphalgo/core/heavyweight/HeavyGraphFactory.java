package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.LongToIntFunction;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    private final ExecutorService threadPool;
    private int labelId;
    private int relationId;
    private int relationCount;
    private int weightId;
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
            relationCount = Math.toIntExact(readOp.relationshipsGetCount());
            weightId = readOp.propertyKeyGetForName(property);
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
            final LongToIntFunction mapOrGet = idMap::mapOrGet;
            withReadOps(readOp -> {
                try (Cursor<NodeItem> cursor = labelId == StatementConstants.NO_SUCH_LABEL
                        ? readOp.nodeCursorGetAll()
                        : readOp.nodeCursorGetForLabel(labelId)) {
                    while (cursor.next()) {
                        final NodeItem node = cursor.get();
                        readNode(node, mapOrGet, matrix);
                    }
                }
            });
        } else {
            final int threads = parallelism(threadPool);
            final int batchSize = nodeCount / threads;

            final List<Runnable> tasks = new ArrayList<>(threads + 1);
            withReadOps(readOp -> {
                final PrimitiveLongIterator nodeIds = labelId == StatementConstants.NO_SUCH_LABEL
                    ? readOp.nodesGetAll()
                    : readOp.nodesGetForLabel(labelId);
                for (int i = 0; i <= threads; i++) {
                    tasks.add(new ImportTask(
                            batchSize,
                            nodeIds,
                            idMap,
                            matrix,
                            weightMapping));
                }
            });
            run(tasks, threadPool);
        }

        return new HeavyGraph(idMap, matrix, weightMapping);
    }

    private static void readNode(
            NodeItem node,
            LongToIntFunction idMap,
            AdjacencyMatrix matrix) {
        final long nodeId = node.id();
        final int vertexId = idMap.applyAsInt(nodeId);
        final int outDegree = node.degree(Direction.OUTGOING);
        final int inDegree = node.degree(Direction.INCOMING);

        matrix.armOut(vertexId, outDegree);
        int outd = 0;
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.OUTGOING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long targetNodeId = rel.endNode();
                final int targetVertexId = idMap.applyAsInt(targetNodeId);
                final long edgeId = rel.id();
                //                try (Cursor<PropertyItem> weights = rel.property(weightPropId)) {
                //                    while (weights.next()) {
                //                        weightMapping.add(edgeId, weights.get().value());
                //                    }
                //                }
                if (++outd > outDegree) {
                    System.out.println("[out] nodeId = " + nodeId);
                }
                matrix.addOutgoing(vertexId, targetVertexId, edgeId);
            }
        }
        matrix.armIn(vertexId, inDegree);
        int ind = 0;
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.INCOMING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long targetNodeId = rel.startNode();
                final int targetVertexId = idMap.applyAsInt(targetNodeId);
                final long edgeId = rel.id();
                if (++ind > inDegree) {
                    System.out.println("[in] nodeId = " + nodeId);
                }
                matrix.addIncoming(targetVertexId, vertexId, edgeId);
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
            final Collection<Runnable> tasks,
            final ExecutorService threadPool) {
        final List<Future<?>> futures = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            futures.add(threadPool.submit(task));
        }

        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                if (!future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException ee) {
                        if (error == null) {
                            error = ee.getCause();
                        } else {
                            error.addSuppressed(ee.getCause());
                        }
                    } catch (CancellationException ignore) {
                    }
                }
            }
            done = true;
        } catch (InterruptedException e) {
            if (error != null) {
                e.addSuppressed(error);
            }
            error = e;
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw new RuntimeException(error);
        }
    }

    private final class ImportTask implements Runnable, Consumer<ReadOperations> {
        private final AdjacencyMatrix matrix;
        private final WeightMapping weightMapping;
        private final LongToIntFunction idMapper;
        private final long[] nodeIds;
        private final int nodeCount;

        ImportTask(
                final int batchSize,
                final PrimitiveLongIterator nodes,
                final IdMap idMap,
                final AdjacencyMatrix matrix,
                final WeightMapping weightMapping) {
            this.matrix = matrix;
            this.weightMapping = weightMapping;
            final long[] ids = new long[batchSize];
            int i;
            for (i = 0; i < batchSize && nodes.hasNext(); i++) {
                final long id = nodes.next();
                ids[i] = id;
                if (!idMap.add(id)) {
                    throw new IllegalArgumentException("duplicate id " + id + " (is " + idMap.get(id) + ")");
                }
            }
            nodeCount = i;
            nodeIds = ids;
            idMapper = idMap::get;
        }

        @Override
        public void run() {
            withReadOps(this);
        }

        @Override
        public void accept(final ReadOperations readOp) {
            final long[] nodeIds = this.nodeIds;
            final int nodeCount = this.nodeCount;
            final LongToIntFunction idMapper = this.idMapper;
            final AdjacencyMatrix matrix = this.matrix;
            for (int i = 0; i < nodeCount; i++) {
                try (Cursor<NodeItem> cursor = readOp.nodeCursor(nodeIds[i])) {
                    if (cursor.next()) {

                        try {
                            HeavyGraphFactory.readNode(
                                    cursor.get(),
                                    idMapper,
                                    matrix);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
