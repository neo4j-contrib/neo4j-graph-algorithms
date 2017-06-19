package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;


/**
 * Partition based parallel PageRank based on
 * "An Efficient Partition-Based Parallel PageRank Algorithm" [1]
 * <p>
 * Each partition thread has its local array of only the nodes that it is responsible for,
 * not for all nodes. Combined, all partitions hold all page rank scores for every node once.
 * Instead of writing partition files and transferring them across the network
 * (as done in the paper since they were concerned with parallelising across multiple nodes),
 * we use a shared {@link AtomicIntegerArray} as destination while upscaling doubles to integer
 * by multiplying with 100_000.
 * <p>
 * Synchronization happens in parallel, too. New scores are written with CAS semantics
 * while the computations steps are still producing data.
 * The dampening and copy step to prepare the new iteration happens in parallel.
 * <p>
 * Partitioning is not done by number of nodes but by the accumulated degree – every partition
 * should have about the same number of relationships to operate on. This is done to avoid
 * having one partition with super nodes and instead have all partitions run in
 * approximately equal time.
 * This partitioning approach is described in
 * "Fast Parallel PageRank: A Linear System Approach" [2].
 * <p>
 * [1]: <a href="http://delab.csd.auth.gr/~dimitris/courses/ir_spring06/page_rank_computing/01531136.pdf">An Efficient Partition-Based Parallel PageRank Algorithm</a><br>
 * [2]: <a href="https://www.cs.purdue.edu/homes/dgleich/publications/gleich2004-parallel.pdf">Fast Parallel PageRank: A Linear System Approach</a>
 */
public class PageRank {

    private final ComputeSteps computeSteps;
    private final boolean isParallel;

    /**
     * Forces sequential use. If you want parallelism, prefer
     * {@link #PageRank(ExecutorService, int, IdMapping, NodeIterator, RelationshipIterator, Degrees, double)}
     */
    public PageRank(
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {
        this(
                null,
                ParallelUtil.DEFAULT_BATCH_SIZE,
                idMapping,
                nodeIterator,
                relationshipIterator,
                degrees,
                dampingFactor);
    }

    /**
     * Parallel Page Rank implementation.
     * Whether the algorithm actually runs in parallel depends on the given
     * executor and batchSize.
     */
    public PageRank(
            ExecutorService executor,
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {

        List<Partition> partitions;
        if (ParallelUtil.canRunInParallel(executor)) {
            partitions = partitionGraph(
                    batchSize,
                    idMapping,
                    nodeIterator,
                    degrees);
        } else {
            executor = null;
            partitions = createSinglePartition(idMapping, degrees);
        }

        final PageRankScorePublisher publisher;
        if (partitions.size() == 1) {
            isParallel = false;
            publisher = new SequentialPublisher(idMapping.nodeCount(), dampingFactor);
        } else {
            isParallel = true;
            publisher = new ParallelPublisher(idMapping.nodeCount(), dampingFactor);
        }
        computeSteps = createComputeSteps(
                publisher,
                relationshipIterator,
                degrees,
                partitions,
                executor);
    }

    /**
     * compute pageRank for n iterations
     */
    public PageRank compute(int iterations) {
        assert iterations >= 1;
        computeSteps.run(iterations);
        return this;
    }

    /**
     * Return the result of the last computation.
     */
    public double[] getPageRank() {
        if (isParallel) {
            int nodeCount = 0;
            for (ComputeStep computeStep : computeSteps) {
                nodeCount += computeStep.nodeCount;
            }
            double[] ranks = new double[nodeCount];
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
            return computeSteps.getFirst().pageRank;
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

    private ComputeSteps createComputeSteps(
            PageRankScorePublisher publisher,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            List<Partition> partitions,
            ExecutorService pool) {
        List<ComputeStep> computeSteps = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            computeSteps.add(new ComputeStep(
                    publisher,
                    relationshipIterator,
                    degrees,
                    partition.pageRank,
                    partition.startNode
            ));
        }
        return new ComputeSteps(computeSteps, publisher, pool);
    }

    private static final class Partition {

        private final int startNode;
        private final int nodeCount;
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
            this.pageRank = new double[nodeCount];
            double startValue = 1.0 / allNodeCount;
            Arrays.fill(pageRank, startValue);
        }
    }

    private static final class ComputeSteps extends AbstractCollection<ComputeStep> {
        private final List<ComputeStep> steps;
        private final PageRankScorePublisher publisher;
        private final ExecutorService pool;

        private ComputeSteps(
                List<ComputeStep> steps,
                PageRankScorePublisher publisher,
                ExecutorService pool) {
            assert steps.size() > 0;
            this.steps = steps;
            this.publisher = publisher;
            this.pool = pool;
        }

        @Override
        public int size() {
            return steps.size();
        }

        @Override
        public Iterator<ComputeStep> iterator() {
            return steps.iterator();
        }

        ComputeStep getFirst() {
            return steps.get(0);
        }

        void run(int iterations) {
            assert iterations >= 1;
            ParallelUtil.run(steps, pool);
            publisher.finishedFirstIteration();
            for (int i = 1; i < iterations; i++) {
                ParallelUtil.run(steps, pool);
            }
            for (ComputeStep step : steps) {
                publisher.synchronizeScores(step.pageRank, step.startNode);
            }
        }
    }

    private static abstract class PageRankScorePublisher {

        private final double alpha;
        private final double dampingFactor;
        private volatile boolean hasScores = false;

        PageRankScorePublisher(double dampingFactor) {
            this.dampingFactor = dampingFactor;
            this.alpha = 1.0 - dampingFactor;
        }

        final void finishedFirstIteration() {
            hasScores = true;
        }

        final void synchronizeScores(
                double[] pageRank,
                int startNode) {
            if (hasScores) {
                synchronizeScores(
                        alpha,
                        dampingFactor,
                        pageRank,
                        startNode,
                        pageRank.length);
            }
        }

        abstract void publish(int nodeId, double score);

        abstract void synchronizeScores(
                double alpha,
                double dampingFactor,
                double[] pageRank,
                int startNode,
                int nodeCount
        );
    }

    private static final class ComputeStep implements Runnable, RelationshipConsumer {

        private final PageRankScorePublisher publisher;
        private final RelationshipIterator relationshipIterator;
        private final Degrees degrees;

        private final double[] pageRank;

        private final int startNode;
        private final int nodeCount;

        private double srcRank;

        ComputeStep(
                PageRankScorePublisher publisher,
                RelationshipIterator relationshipIterator,
                Degrees degrees,
                double[] pageRank,
                int startNode) {
            this.publisher = publisher;
            this.relationshipIterator = relationshipIterator;
            this.degrees = degrees;
            this.startNode = startNode;
            this.nodeCount = pageRank.length;
            this.pageRank = pageRank;

        }

        @Override
        public void run() {
            publisher.synchronizeScores(pageRank, startNode);
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
    }

    private static final class ParallelPublisher extends PageRankScorePublisher {

        private AtomicIntegerArray scores;

        private ParallelPublisher(int nodeCount, double dampingFactor) {
            super(dampingFactor);
            scores = new AtomicIntegerArray(nodeCount);
        }

        @Override
        void publish(int nodeId, double score) {
            scores.getAndAdd(nodeId, (int) (100_000 * score));
        }

        @Override
        void synchronizeScores(
                double alpha,
                double dampingFactor,
                double[] pageRank,
                int startNode,
                int nodeCount) {
            int end = startNode + nodeCount;
            for (int i = startNode; i < end; i++) {
                int sum = scores.getAndSet(i, 0);
                pageRank[i - startNode] = alpha + dampingFactor * (sum / 100_000.0);
            }
        }
    }

    private static final class SequentialPublisher extends PageRankScorePublisher {
        private final int[] scores;

        private SequentialPublisher(int nodeCount, double dampingFactor) {
            super(dampingFactor);
            this.scores = new int[nodeCount];
        }

        @Override
        void publish(final int nodeId, final double score) {
            scores[nodeId] += (int) (100_000 * score);
        }

        @Override
        void synchronizeScores(
                final double alpha,
                final double dampingFactor,
                final double[] pageRank,
                final int startNode,
                final int nodeCount) {
            int[] scores = this.scores;
            int len = scores.length;
            for (int i = 0; i < len; i++) {
                pageRank[i] = alpha + dampingFactor * (scores[i] / 100_000.0);
                scores[i] = 0;
            }
        }
    }
}
