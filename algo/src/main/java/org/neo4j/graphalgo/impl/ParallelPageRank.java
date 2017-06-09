package org.neo4j.graphalgo.impl;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.NamedThreadFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Partition based parallel PageRank based on
 * "An Efficient Partition-Based Parallel PageRank Algorithm" [1]
 * <p>
 * Each partition thread has its local array of only the nodes that it is responsible for,
 * not for all nodes. Combined, all partitions hold all page rank scores for every node once.
 * Instead of writing partition files and transferring them across the network
 * (as done in the paper since they were concerned with parallelising across multiple nodes),
 * we use a ring buffer as an MPSC queue.
 * <p>
 * Synchronization happens partly in parallel. New scores are collected and written in a dedicated thread
 * while the computations steps are still producing data. The dampening and copy step to prepare the new iteration
 * happens in parallel.
 * The synchronizing threads has a copy of every page ranks score as well, as calculated
 * scores of an iteration must not affect the current iteration.
 * Instead of writing into a single array, separate arrays for every partition are
 * maintained in order to avoid false sharing and visibility issues during the parallel pre-iteration-copy.
 * <p>
 * Partitioning is not done by number of nodes but by the accumulated degree – every partition
 * should have about the same number of relationships to operate on. This is done to avoid
 * having one partition with super nodes and instead have all partitions run in
 * approximately equal time.
 * This is described in "Fast Parallel PageRank: A Linear System Approach" [2].
 * <p>
 * [1]: <a href="http://delab.csd.auth.gr/~dimitris/courses/ir_spring06/page_rank_computing/01531136.pdf">An Efficient Partition-Based Parallel PageRank Algorithm</a><br>
 * [2]: <a href="https://www.cs.purdue.edu/homes/dgleich/publications/gleich2004-parallel.pdf">Fast Parallel PageRank: A Linear System Approach</a>
 */
public class ParallelPageRank implements AutoCloseable {

    private final ExecutorService executor;
    private final List<ComputeStep> computeSteps;
    private final SyncPartitions handler;
    private final Disruptor<PageRankScorePair> disruptor;

    // synchronizing data for every iteration always happens on a
    // dedicated thread, started by the disruptor
    private static final ThreadFactory SYNC_THREAD =
            NamedThreadFactory.daemon("pagerank-par-sync");

    public ParallelPageRank(
            ExecutorService executor,
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {

        List<Partition> partitions;
        if (ParallelUtil.canRunInParallel(executor)) {
            this.executor = executor;
            partitions = partitionGraph(
                    batchSize,
                    idMapping,
                    nodeIterator,
                    degrees);
        } else {
            this.executor = null;
            partitions = createSinglePartition(idMapping, degrees);
        }

        if (partitions.size() == 1) {
            handler = null;
            disruptor = null;
            computeSteps = createSingleComputeStep(
                    partitions.get(0),
                    dampingFactor,
                    relationshipIterator,
                    degrees
            );
        } else {
            handler = createPartitionSyncHandler(partitions);
            disruptor = startDisruptor(executor, handler);
            computeSteps = createComputeSteps(
                relationshipIterator,
                degrees,
                dampingFactor,
                partitions,
                disruptor.getRingBuffer());
        }
    }

    /**
     * compute pageRank for n iterations
     */
    public ParallelPageRank compute(int iterations) {
        assert iterations >= 1;
        if (handler == null) {
            return computeSequential(iterations);
        }
        double[][] pageRanks = handler.pageRanks;
        int partitionSize = pageRanks.length;

        runOneIteration();

        for (int i = 1; i < iterations; i++) {
            for (int j = 0; j < partitionSize; j++) {
                computeSteps.get(j).prepareNextIteration(pageRanks[j]);
            }

            runOneIteration();
        }

        for (int j = 0; j < partitionSize; j++) {
            computeSteps.get(j).synchronizeScores(pageRanks[j]);
        }

        return this;
    }

    private ParallelPageRank computeSequential(int iterations) {
        assert computeSteps.size() == 1;

        final ComputeStep step = computeSteps.get(0);
        final SequentialPublisher publisher = (SequentialPublisher) step.publisher;
        step.run();

        for (int i = 1; i < iterations; i++) {
            step.prepareNextIteration(publisher.scores);
            step.run();
        }

        step.synchronizeScores(publisher.scores);

        return this;
    }

    public double[] getPageRank() {
        if (handler != null) {
            double[] ranks = new double[handler.nodeCount];
            for (ComputeStep computeStep : computeSteps) {
                double[] scores = computeStep.pageRank;
                System.arraycopy(
                        scores,
                        0,
                        ranks,
                        computeStep.startNode,
                        computeStep.nodeCount);
            }
            return ranks;
        } else {
            assert computeSteps.size() == 1;
            return computeSteps.get(0).pageRank;
        }
    }

    @Override
    public void close() {
        if (disruptor != null) {
            disruptor.shutdown();
        }
    }

    private List<Partition> partitionGraph(
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            Degrees degrees) {
        int nodeCount = idMapping.nodeCount();
        PrimitiveIntIterator nodes = nodeIterator.nodeIterator();
        List<Partition> partitions = new ArrayList<>();
        int start = 0;
        while (nodes.hasNext()) {
            Partition partition = new Partition(
                    nodeCount,
                    nodes,
                    degrees,
                    start,
                    batchSize);
            partitions.add(partition);
            start += partition.nodeCount;
        }
        return partitions;
    }

    private List<Partition> createSinglePartition(
            IdMapping idMapping,
            Degrees degrees) {
        return Collections.singletonList(
                new Partition(
                        idMapping.nodeCount(),
                        null,
                        degrees,
                        0,
                        -1
                )
        );
    }

    private List<ComputeStep> createSingleComputeStep(
            Partition partition,
            final double dampingFactor,
            final RelationshipIterator relationshipIterator,
            final Degrees degrees) {
        return Collections.singletonList(
                new ComputeStep(
                        new SequentialPublisher(partition.nodeCount),
                        dampingFactor,
                        relationshipIterator,
                        degrees,
                        partition.pageRank,
                        partition.startNode
                )
        );
    }

    private SyncPartitions createPartitionSyncHandler(
            List<Partition> partitions) {
        int nodeCount = 0;
        int eventCount = 0;
        int numberOfPartitions = partitions.size();
        double[][] pageRanks = new double[numberOfPartitions][];
        int[] nodeStarts = new int[numberOfPartitions];
        for (int i = 0; i < numberOfPartitions; i++) {
            Partition partition = partitions.get(i);
            pageRanks[i] = new double[partition.nodeCount];
            nodeStarts[i] = partition.startNode;
            nodeCount += partition.nodeCount;
            eventCount += partition.partitionSize;
        }
        assert nodeStarts[0] == 0;
        return new SyncPartitions(nodeCount, eventCount, pageRanks, nodeStarts);
    }

    private Disruptor<PageRankScorePair> startDisruptor(
            ExecutorService executor,
            SyncPartitions handler) {
        int parallelism;
        if (executor instanceof ThreadPoolExecutor) {
            parallelism = ((ThreadPoolExecutor) executor).getMaximumPoolSize();
        } else if (executor instanceof ForkJoinPool) {
            parallelism = ((ForkJoinPool) executor).getParallelism();
        } else {
            parallelism = 1;
        }

        // pre-allocate between 32 and 32768 elements for the ring buffer
        // depending on possible thread size with exponential growth.
        parallelism = Math.min(15, Math.max(5, parallelism));
        int ringBufferSize = 1 << parallelism;

        Disruptor<PageRankScorePair> disruptor = new Disruptor<>(
                PageRankScorePair::new,
                ringBufferSize,
                SYNC_THREAD,
                ProducerType.MULTI,
                new BusySpinWaitStrategy());

        disruptor.handleEventsWith(handler);
        disruptor.start();

        return disruptor;
    }

    private List<ComputeStep> createComputeSteps(
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor,
            List<Partition> partitions,
            RingBuffer<PageRankScorePair> ringBuffer) {
        List<ComputeStep> computeSteps = new ArrayList<>();
        for (Partition partition : partitions) {
            computeSteps.add(new ComputeStep(
                    new ParallelPublisher(ringBuffer),
                    dampingFactor,
                    relationshipIterator,
                    degrees,
                    partition.pageRank,
                    partition.startNode
            ));
        }
        return computeSteps;
    }

    private void runOneIteration() {
        handler.newIteration();
        ParallelUtil.run(computeSteps, executor);
        handler.waitForAllEvents();
    }

    private static final class PageRankScorePair {
        private int nodeId;
        private double score;
    }

    private static final class Partition {

        private final int startNode;
        private final int nodeCount;
        private final int partitionSize;
        private final double[] pageRank;

        Partition(
                int allNodeCount,
                PrimitiveIntIterator nodes,
                Degrees degrees,
                int startNode,
                int batchSize) {

            int nodeCount;
            int partitionSize = 0;
            if (batchSize > 0) {
                nodeCount = 0;
                while (partitionSize < batchSize && nodes.hasNext()) {
                    int nodeId = nodes.next();
                    ++nodeCount;
                    partitionSize += degrees.degree(nodeId, Direction.OUTGOING);
                }
            } else {
                nodeCount = allNodeCount;
            }

            this.startNode = startNode;
            this.nodeCount = nodeCount;
            this.partitionSize = partitionSize;
            this.pageRank = new double[nodeCount];
            double startValue = 1.0 / allNodeCount;
            Arrays.fill(pageRank, startValue);
        }
    }

    private interface PageRankScorePublisher {
        void publish(int nodeId, double score);
    }

    private static final class ComputeStep implements Runnable, RelationshipConsumer {

        private final PageRankScorePublisher publisher;
        private final RelationshipIterator relationshipIterator;
        private final Degrees degrees;

        private final double dampingFactor;
        private final double alpha;
        private final double[] pageRank;
        private double[] prevRank;

        private final int startNode;
        private final int nodeCount;

        private double srcRank;

        ComputeStep(
                PageRankScorePublisher publisher,
                double dampingFactor,
                RelationshipIterator relationshipIterator,
                Degrees degrees,
                double[] pageRank,
                int startNode) {
            this.publisher = publisher;
            this.relationshipIterator = relationshipIterator;
            this.degrees = degrees;
            this.dampingFactor = dampingFactor;
            this.alpha = 1.0 - dampingFactor;
            this.startNode = startNode;
            this.nodeCount = pageRank.length;
            this.pageRank = pageRank;

        }

        @Override
        public void run() {
            if (prevRank != null) {
                synchronizeScores(prevRank);
                prevRank = null;
            }

            int endNode = nodeCount + startNode;
            for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                srcRank = degree == 0 ? 0 : pageRank[nodeId - startNode] / degree;
                relationshipIterator.forEachRelationship(
                        nodeId,
                        Direction.OUTGOING,
                        this);
            }
        }

        @Override
        public boolean accept(
                int sourceNodeId,
                int targetNodeId,
                long relationId) {
            publisher.publish(targetNodeId, srcRank);
            return true;
        }

        void prepareNextIteration(double[] pageRank) {
            prevRank = pageRank;
        }

        void synchronizeScores(double[] prevRank) {
            int len = prevRank.length;
            for (int i = 0; i < len; i++) {
                pageRank[i] = alpha + dampingFactor * prevRank[i];
                prevRank[i] = 0.0;
            }
        }
    }

    private static final class ParallelPublisher implements PageRankScorePublisher {

        private final RingBuffer<PageRankScorePair> ringBuffer;

        private ParallelPublisher(RingBuffer<PageRankScorePair> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void publish(int nodeId, double score) {
            long sequence = ringBuffer.next();
            try {
                PageRankScorePair pair = ringBuffer.get(sequence);
                pair.nodeId = nodeId;
                pair.score = score;
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }

    private static final class SequentialPublisher implements PageRankScorePublisher {
        private final double[] scores;

        private SequentialPublisher(int nodeCount) {
            this.scores = new double[nodeCount];
        }

        @Override
        public void publish(final int nodeId, final double score) {
            scores[nodeId] += score;
        }
    }

    private static final class SyncPartitions implements EventHandler<PageRankScorePair> {

        private final int nodeCount;
        private final int expectedEvents;
        private final int[] nodeStarts;
        private final double[][] pageRanks;

        private volatile int currentEvents;

        private SyncPartitions(
                int nodeCount,
                int expectedEvents,
                double[][] pageRanks,
                int[] nodeStarts) {
            this.nodeCount = nodeCount;
            this.expectedEvents = expectedEvents;
            this.pageRanks = pageRanks;
            this.nodeStarts = nodeStarts;
        }

        @Override
        public void onEvent(
                PageRankScorePair event,
                long sequence,
                boolean endOfBatch) throws Exception {
            int index = batchIndex(event.nodeId, nodeStarts);
            pageRanks[index][event.nodeId - nodeStarts[index]] += event.score;

            if (++currentEvents >= expectedEvents) {
                signalAllEvents();
            }
        }

        void newIteration() {
            currentEvents = 0;
        }

        void waitForAllEvents() {
            synchronized (this) {
                while (currentEvents < expectedEvents) {
                    try {
                        this.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        private void signalAllEvents() {
            synchronized (this) {
                this.notify();
            }
        }

        private static int batchIndex(int nodeId, int[] nodeStarts) {
            int low = 0;
            int high = nodeStarts.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int midVal = nodeStarts[mid];

                if (midVal < nodeId) {
                    low = mid + 1;
                } else if (midVal > nodeId) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }

            return low - 1;
        }
    }
}
