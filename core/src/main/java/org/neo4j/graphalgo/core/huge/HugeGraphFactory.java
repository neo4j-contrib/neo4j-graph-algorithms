package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.HugeNullWeightMap;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.paged.LongArray;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

public final class HugeGraphFactory extends GraphFactory {

    private final ExecutorService threadPool;
    private long nodeCount;
    private long allNodesCount;
    private long maxRelCount;

    private int labelId;
    private int[] relationId;
    private int weightId;

    public HugeGraphFactory(
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
            weightId = setup.loadDefaultRelationshipWeight()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
            nodeCount = readOp.countsForNode(labelId);
            allNodesCount = readOp.nodesGetCount();
            maxRelCount = Math.max(
                    readOp.countsForRelationshipWithoutTxState(labelId, relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0], ReadOperations.ANY_LABEL),
                    readOp.countsForRelationshipWithoutTxState(ReadOperations.ANY_LABEL, relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0], labelId)
            );
        });
    }

    @Override
    public HugeGraph build() {
        int concurrency = setup.concurrency();
        int batchSize = setup.batchSize;
        AllocationTracker tracker = setup.tracker;
        ImportProgress progress = new ImportProgress(
                progressLogger,
                tracker,
                nodeCount,
                maxRelCount,
                setup.loadIncoming,
                setup.loadOutgoing);


        HugeWeightMapping weights = weightMapping(tracker);
        HugeIdMap mapping = loadNodes(concurrency, batchSize, tracker, progress);
        HugeGraph graph = loadRelationships(mapping, weights, concurrency, batchSize, tracker, progress);
        progressLogger.logDone(tracker);
        return graph;
    }

    private HugeWeightMapping weightMapping(AllocationTracker tracker) {
        return weightId == StatementConstants.NO_SUCH_PROPERTY_KEY
                    ? new HugeNullWeightMap(setup.relationDefaultWeight)
                    : new HugeWeightMap(nodeCount, setup.relationDefaultWeight, tracker);
    }

    private HugeIdMap loadNodes(
            int concurrency,
            int batchSize,
            AllocationTracker tracker,
            ImportProgress progress) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            return loadSequential(tracker, progress);
        }

        // start loading ids early in a single producer
        NodesStoreLoader queue = new NodesStoreLoader(
                api,
                labelId,
                concurrency,
                HugeIdMap.PAGE_SIZE);
        Future<?> queueThread = threadPool.submit(queue);


        // larger batch size so that only `concurrency` tasks are created
        // as tasks are working a queue, we don't run in mis-sized threads
        long newBatchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                batchSize);

        // the number of final pages that need to be created
        int expectedPages = Math.toIntExact(ParallelUtil.threadSize(
                HugeIdMap.PAGE_SIZE,
                nodeCount));

        // the number of final pages that need to be created for all nodes
        // since we could map sparse nodes to the highest id, we have to
        // create pages that could hold _all_ nodes
        int allExpectedPages = Math.toIntExact(ParallelUtil.threadSize(
                HugeIdMap.PAGE_SIZE,
                allNodesCount));

        long[][] algoToNeoPages = new long[expectedPages][];
        LongLongMap[] neoToAlgoPages = new LongLongMap[allExpectedPages];
        tracker.add(sizeOfObjectArray(expectedPages));
        tracker.add(sizeOfObjectArray(allExpectedPages));

        Collection<NodeIdMapImporter> tasks = LazyBatchCollection.ofCached(
                nodeCount,
                newBatchSize,
                (start, len) -> new NodeIdMapImporter(
                        HugeIdMap.PAGE_SIZE,
                        queue.queue(),
                        algoToNeoPages,
                        neoToAlgoPages,
                        tracker,
                        progress
                ));
        ParallelUtil.runWithConcurrency(concurrency, tasks, threadPool);
        try {
            queueThread.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        HugeLongLongMap idMap = HugeLongLongMap.fromPages(allNodesCount, neoToAlgoPages, tracker);
        return new HugeIdMap(nodeCount, idMap, algoToNeoPages, tracker);
    }

    private HugeIdMap loadSequential(AllocationTracker tracker, ImportProgress progress) {
        final HugeIdMap mapping = new HugeIdMap(nodeCount, tracker);
        withReadOps(readOp -> {
            final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                    ? readOp.nodesGetAll()
                    : readOp.nodesGetForLabel(labelId);
            long nodes = 0;
            while (nodeIds.hasNext()) {
                mapping.add(nodeIds.next());
                progress.nodeProgress(++nodes);
            }
            mapping.buildMappedIds();
        });
        return mapping;
    }

    private HugeGraph loadRelationships(
            HugeIdMap mapping,
            HugeWeightMapping weights,
            int concurrency,
            int batchSize,
            AllocationTracker tracker,
            ImportProgress progress) {
        boolean loadsAnything = false;
        LongArray inOffsets = null;
        LongArray outOffsets = null;
        ByteArray inAdjacency = null;
        ByteArray outAdjacency = null;
        if (setup.loadIncoming) {
            inOffsets = LongArray.newArray(nodeCount, tracker);
            inAdjacency = ByteArray.newArray(0, tracker);
            inAdjacency.skipAllocationRegion(1);
            loadsAnything = true;
        }
        if (setup.loadOutgoing) {
            outOffsets = LongArray.newArray(nodeCount, tracker);
            outAdjacency = ByteArray.newArray(nodeCount, tracker);
            outAdjacency.skipAllocationRegion(1);
            loadsAnything = true;
        }
        if (loadsAnything) {
            // needs final b/c of reference from lambda
            final LongArray finalInOffsets = inOffsets;
            final LongArray finalOutOffsets = outOffsets;
            final ByteArray finalInAdjacency = inAdjacency;
            final ByteArray finalOutAdjacency = outAdjacency;
            progress.resetForRelationships();
            ParallelUtil.readParallel(
                    concurrency,
                    batchSize,
                    mapping,
                    (offset, nodeIds) -> new RelationshipImporter(
                            api,
                            progress,
                            mapping,
                            weights,
                            nodeIds,
                            finalInOffsets,
                            finalOutOffsets,
                            finalInAdjacency,
                            finalOutAdjacency,
                            relationId,
                            weightId,
                            offset
                    ),
                    threadPool);
        }

        return new HugeGraphImpl(
                tracker,
                mapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }
}
