package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

final class NodesBasedImporter implements Runnable {

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final GraphDimensions dimensions;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    NodesBasedImporter(
            final GraphSetup setup,
            final GraphDatabaseAPI api,
            final GraphDimensions dimensions,
            final ImportProgress progress,
            final AllocationTracker tracker,
            final HugeIdMap idMap,
            final HugeWeightMapBuilder weights,
            final HugeAdjacencyBuilder outAdjacency,
            final HugeAdjacencyBuilder inAdjacency,
            final ExecutorService threadPool,
            final int concurrency) {
        this.setup = setup;
        this.api = api;
        this.dimensions = dimensions;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    @Override
    public void run() {
        loadRelationships();
    }

    private void loadRelationships() {
        if (!setup.loadAsUndirected && !setup.loadIncoming && !setup.loadOutgoing) {
            return;
        }

        long nodeCount = dimensions.hugeNodeCount();
        NodeQueue nodes = new NodeQueue(nodeCount);
        int concurrency = this.concurrency;

        int batchSize = BitUtil.previousPowerOfTwo((int) Math.min(
                (long) Integer.MAX_VALUE,
                ParallelUtil.threadSize(concurrency, nodeCount)
        ));
        int pages = Math.toIntExact(ParallelUtil.threadSize(batchSize, nodeCount));

        WeightBuilder weightBuilder = WeightBuilder.of(weights, pages, batchSize, tracker);
        AdjacencyBuilder outBuilder = AdjacencyBuilder.of(outAdjacency, pages, batchSize, tracker);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.of(inAdjacency, pages, batchSize, tracker);

        for (int idx = 0; idx < pages; idx++) {
            weightBuilder.addWeightImporter(idx);
            outBuilder.addAdjacencyImporter(tracker, idx);
            inBuilder.addAdjacencyImporter(tracker, idx);
        }
        weightBuilder.finish();
        outBuilder.finish();
        inBuilder.finish();

        HugeRelationshipImporter[] tasks = new HugeRelationshipImporter[concurrency];
        Arrays.setAll(tasks, idx -> new HugeRelationshipImporter(
                nodes,
                weightBuilder,
                outBuilder,
                inBuilder,
                idx,
                setup.loadAsUndirected
        ));
        ParallelUtil.run(Arrays.asList(tasks), threadPool);
    }

    private static final class NodeQueue {
        private final AtomicLong current = new AtomicLong();
        private final long max;

        NodeQueue(final long max) {
            this.max = max;
        }

        long next() {
            long nodeId = current.getAndIncrement();
            return nodeId < max ? nodeId : -1L;
        }
    }

    private final class HugeRelationshipImporter extends StatementAction {
        private final NodeQueue nodes;
        private final WeightBuilder weightBuilder;
        private final AdjacencyBuilder outBuilder;
        private final AdjacencyBuilder inBuilder;
        private final int batchIndex;
        private final boolean undirected;

        HugeRelationshipImporter(
                NodeQueue nodes,
                WeightBuilder weightBuilder,
                AdjacencyBuilder outBuilder,
                AdjacencyBuilder inBuilder,
                int batchIndex,
                boolean undirected) {
            super(NodesBasedImporter.this.api);
            this.nodes = nodes;
            this.weightBuilder = weightBuilder;
            this.outBuilder = outBuilder;
            this.inBuilder = inBuilder;
            this.batchIndex = batchIndex;
            this.undirected = undirected;
        }

        @Override
        public String threadName() {
            return "HugeRelationshipImport-" + batchIndex;
        }

        @Override
        public void accept(final KernelTransaction transaction) {
            NodeQueue nodes = this.nodes;
            HugeIdMap idMap = NodesBasedImporter.this.idMap;
            ImportProgress progress = NodesBasedImporter.this.progress;
            WeightBuilder weightBuilder = this.weightBuilder;
            AdjacencyBuilder outBuilder = this.outBuilder;
            AdjacencyBuilder inBuilder = this.inBuilder;
            Read readOp = transaction.dataRead();
            RelationshipLoader loader = relationshipLoader(
                    transaction,
                    idMap,
                    dimensions.relationshipTypeId());
            try (NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor()) {
                long nodeId, sourceNodeId;
                int imported;
                while ((nodeId = nodes.next()) != -1L) {
                    sourceNodeId = idMap.toOriginalNodeId(nodeId);
                    readOp.singleNode(sourceNodeId, nodeCursor);
                    if (nodeCursor.next()) {
                        imported = loader.load(
                                nodeCursor,
                                nodeId,
                                weightBuilder,
                                outBuilder,
                                inBuilder
                        );
                        progress.relationshipsImported(imported);
                    }
                }
            }
        }

        private RelationshipLoader relationshipLoader(
                final KernelTransaction transaction,
                final HugeIdMap idMap,
                final int[] relationId) {
            final Read readOp = transaction.dataRead();
            final CursorFactory cursors = transaction.cursors();

            final RelationshipLoader loader;
            if (undirected) {
                final VisitRelationship visitIn;
                final VisitRelationship visitOut;
                if (weights.loadsWeights()) {
                    visitIn = new VisitIncomingNoWeight(idMap);
                    visitOut = new VisitUndirectedOutgoingWithWeight(readOp, cursors, idMap);
                } else {
                    visitIn = new VisitIncomingNoWeight(idMap);
                    visitOut = new VisitOutgoingNoWeight(idMap);
                }
                loader = new ReadUndirected(transaction, relationId, visitOut, visitIn);
            } else {
                RelationshipLoader load = null;
                if (outAdjacency != null) {
                    final VisitRelationship visitOut;
                    if (weights.loadsWeights()) {
                        visitOut = new VisitOutgoingWithWeight(readOp, cursors, idMap);
                    } else {
                        visitOut = new VisitOutgoingNoWeight(idMap);
                    }
                    load = new ReadOutgoing(transaction, relationId, visitOut);
                }
                if (inAdjacency != null) {
                    final VisitRelationship visitIn;
                    if (weights.loadsWeights()) {
                        visitIn = new VisitIncomingWithWeight(readOp, cursors, idMap);
                    } else {
                        visitIn = new VisitIncomingNoWeight(idMap);
                    }
                    if (load != null) {
                        load = new ReadBoth((ReadOutgoing) load, visitIn);
                    } else {
                        load = new ReadIncoming(transaction, relationId, visitIn);
                    }
                }

                if (load != null) {
                    loader = load;
                } else {
                    loader = new ReadNothing(transaction, relationId);
                }
            }
            return loader;
        }
    }

}
