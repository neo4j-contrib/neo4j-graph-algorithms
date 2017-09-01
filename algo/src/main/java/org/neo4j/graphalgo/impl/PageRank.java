package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.PageRankResult;
import org.neo4j.graphalgo.exporter.PageRankResultExporter;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;


/**
 * Partition based parallel PageRank based on
 * "An Efficient Partition-Based Parallel PageRank Algorithm" [1]
 * <p>
 * Each partition thread has its local array of only the nodes that it is responsible for,
 * not for all nodes. Combined, all partitions hold all page rank scores for every node once.
 * Instead of writing partition files and transferring them across the network
 * (as done in the paper since they were concerned with parallelising across multiple nodes),
 * we use integer arrays to write the results to.
 * The actual score is upscaled from a double to an integer by multiplying it with {@code 100_000}.
 * <p>
 * To avoid contention by writing to a shared array, we partition the result array.
 * During execution, the scores arrays
 * are shaped like this:
 * <pre>
 *     [ executing partition ] -> [ calculated partition ] -> [ local page rank scores ]
 * </pre>
 * Each single partition writes in a partitioned array, calculation the scores
 * for every receiving partition. A single partition only sees:
 * <pre>
 *     [ calculated partition ] -> [ local page rank scores ]
 * </pre>
 * The coordinating thread then builds the transpose of all written partitions from every partition:
 * <pre>
 *     [ calculated partition ] -> [ executing partition ] -> [ local page rank scores ]
 * </pre>
 * This step does not happen in parallel, but does not involve extensive copying.
 * The local page rank scores needn't be copied, only the partitioning arrays.
 * All in all, {@code concurrency^2} array element reads and assignments have to
 * be performed.
 * <p>
 * For the next iteration, every partition first updates its scores, in parallel.
 * A single partition now sees:
 * <pre>
 *     [ executing partition ] -> [ local page rank scores ]
 * </pre>
 * That is, a list of all calculated scores for it self, grouped by the partition that
 * calculated these scores.
 * This means, most of the synchronization happens in parallel, too.
 * <p>
 * Partitioning is not done by number of nodes but by the accumulated degree –
 * as described in "Fast Parallel PageRank: A Linear System Approach" [2].
 * Every partition should have about the same number of relationships to operate on.
 * This is done to avoid having one partition with super nodes and instead have
 * all partitions run in approximately equal time.
 * Smaller partitions are merged down until we have at most {@code concurrency} partitions,
 * in order to batch partitions and keep the number of threads in use predictable/configurable.
 * <p>
 * [1]: <a href="http://delab.csd.auth.gr/~dimitris/courses/ir_spring06/page_rank_computing/01531136.pdf">An Efficient Partition-Based Parallel PageRank Algorithm</a><br>
 * [2]: <a href="https://www.cs.purdue.edu/homes/dgleich/publications/gleich2004-parallel.pdf">Fast Parallel PageRank: A Linear System Approach</a>
 */
public class PageRank extends Algorithm<PageRank> implements PageRankAlgorithm {

    private final ComputeSteps computeSteps;
    private final IdMapping idMapping;

    /**
     * Forces sequential use. If you want parallelism, prefer
     * {@link #PageRank(ExecutorService, int, int, IdMapping, NodeIterator, RelationshipIterator, Degrees, double)}
     */
    PageRank(
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {
        this(
                null,
                -1,
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
    PageRank(
            ExecutorService executor,
            int concurrency,
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            double dampingFactor) {
        this.idMapping = idMapping;
        List<Partition> partitions;
        if (ParallelUtil.canRunInParallel(executor)) {
            partitions = partitionGraph(
                    adjustBatchSize(batchSize),
                    idMapping,
                    nodeIterator,
                    degrees);
        } else {
            executor = null;
            partitions = createSinglePartition(idMapping, degrees);
        }

        computeSteps = createComputeSteps(
                concurrency,
                idMapping.nodeCount(),
                dampingFactor,
                relationshipIterator,
                degrees,
                partitions,
                executor);
    }

    /**
     * compute pageRank for n iterations
     */
    @Override
    public PageRank compute(int iterations) {
        assert iterations >= 1;
        computeSteps.run(iterations);
        return this;
    }

    @Override
    public PageRankResult result() {
        return computeSteps.getPageRank();
    }

    @Override
    public Algorithm<?> algorithm() {
        return this;
    }

    private int adjustBatchSize(int batchSize) {
        // multiply batchsize by 8 as a very rough estimate of an average
        // degree of 8 for nodes, so that every partition has approx
        // batchSize nodes.
        batchSize <<= 3;
        return batchSize > 0 ? batchSize : Integer.MAX_VALUE;
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
            int concurrency,
            int nodeCount,
            double dampingFactor,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            List<Partition> partitions,
            ExecutorService pool) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_QUEUE_SIZE;
        }
        final int expectedParallelism = Math.min(
                concurrency,
                partitions.size());
        List<ComputeStep> computeSteps = new ArrayList<>(expectedParallelism);
        IntArrayList starts = new IntArrayList(expectedParallelism);
        IntArrayList lengths = new IntArrayList(expectedParallelism);
        int partitionsPerThread = ParallelUtil.threadSize(
                concurrency + 1,
                partitions.size());
        Iterator<Partition> parts = partitions.iterator();

        while (parts.hasNext()) {
            Partition partition = parts.next();
            int partitionCount = partition.nodeCount;
            int start = partition.startNode;
            for (int i = 1; i < partitionsPerThread && parts.hasNext(); i++) {
                partition = parts.next();
                partitionCount += partition.nodeCount;
            }

            double[] partitionRank = new double[partitionCount];
            Arrays.fill(partitionRank, 1.0 / nodeCount);
            starts.add(start);
            lengths.add(partitionCount);

            computeSteps.add(new ComputeStep(
                    dampingFactor,
                    relationshipIterator,
                    degrees,
                    partitionRank,
                    start
            ));
        }

        int[] startArray = starts.toArray();
        int[] lengthArray = lengths.toArray();
        for (ComputeStep computeStep : computeSteps) {
            computeStep.setStarts(startArray, lengthArray);
        }
        return new ComputeSteps(concurrency, computeSteps, pool);
    }

    private static int idx(int id, int ids[]) {
        int length = ids.length;

        int low = 0;
        int high = length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = ids[mid];

            if (midVal < id) {
                low = mid + 1;
            } else if (midVal > id) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low - 1;
    }

    @Override
    public PageRank me() {
        return this;
    }

    @Override
    public PageRank release() {

        return this;
    }

    private static final class Partition {

        private final int startNode;
        private final int nodeCount;

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
        }
    }

    private final class ComputeSteps {
        private final int concurrency;
        private final List<ComputeStep> steps;
        private final ExecutorService pool;
        private final int[][][] scores;

        private ComputeSteps(
                int concurrency,
                List<ComputeStep> steps,
                ExecutorService pool) {
            assert !steps.isEmpty();
            this.concurrency = concurrency;
            this.steps = steps;
            this.pool = pool;
            int stepSize = steps.size();
            scores = new int[stepSize][][];
            Arrays.setAll(scores, i -> new int[stepSize][]);
        }

        PageRankResult getPageRank() {
            ComputeStep firstStep = steps.get(0);
            if (steps.size() == 1) {
                return new PrimitiveDoubleArrayResult(idMapping, firstStep.pageRank);
            }
            double[][] results = new double[steps.size()][];
            Iterator<ComputeStep> iterator = steps.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                results[i++] = iterator.next().pageRank;
            }
            return new PartitionedPrimitiveDoubleArrayResult(
                    idMapping,
                    results,
                    firstStep.starts
            );
        }

        private void run(int iterations) {
            for (int i = 0; i < iterations && running(); i++) {
                // calculate scores
                ParallelUtil.runWithConcurrency(concurrency, steps, pool);
                synchronizeScores();
                // sync scores
                ParallelUtil.runWithConcurrency(concurrency, steps, pool);
            }
        }

        private void synchronizeScores() {
            int stepSize = steps.size();
            int[][][] scores = this.scores;
            int i;
            for (i = 0; i < stepSize; i++) {
                synchronizeScores(steps.get(i), i, scores);
            }
        }

        private void synchronizeScores(
                ComputeStep step,
                int idx,
                int[][][] scores) {
            step.prepareNextIteration(scores[idx]);
            int[][] nextScores = step.nextScores;
            for (int j = 0, len = nextScores.length; j < len; j++) {
                scores[j][idx] = nextScores[j];
            }
        }
    }

    private interface Behavior {
        void run();
    }


    private static final class ComputeStep implements Runnable, RelationshipConsumer {

        private int[] starts;
        private final RelationshipIterator relationshipIterator;
        private final Degrees degrees;

        private final double alpha;
        private final double dampingFactor;

        private final double[] pageRank;
        private int[][] nextScores;
        private int[][] prevScores;

        private final int startNode;
        private final int endNode;
        private final int nodeCount;

        private int[] srcRank = new int[1];
        private Behavior behavior;

        private Behavior runs = this::runsIteration;
        private Behavior syncs = this::subsequentSync;

        ComputeStep(
                double dampingFactor,
                RelationshipIterator relationshipIterator,
                Degrees degrees,
                double[] pageRank,
                int startNode) {
            this.dampingFactor = dampingFactor;
            this.alpha = 1.0 - dampingFactor;
            this.relationshipIterator = relationshipIterator;
            this.degrees = degrees;
            this.startNode = startNode;
            this.nodeCount = pageRank.length;
            this.endNode = startNode + pageRank.length;
            this.pageRank = pageRank;
            this.behavior = runs;
        }

        void setStarts(int starts[], int[] lengths) {
            this.starts = starts;
            this.nextScores = new int[starts.length][];
            Arrays.setAll(nextScores, i -> new int[lengths[i]]);
        }

        @Override
        public void run() {
            behavior.run();
        }

        private void runsIteration() {
            singleIteration();
            behavior = syncs;
        }

        private void singleIteration() {
            int startNode = this.startNode;
            int endNode = this.endNode;
            int[] srcRank = this.srcRank;
            RelationshipIterator rels = this.relationshipIterator;
            for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
                int rank = calculateRank(nodeId, startNode);
                if (rank != 0) {
                    srcRank[0] = rank;
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }

        @Override
        public boolean accept(
                int sourceNodeId,
                int targetNodeId,
                long relationId) {
            int rank = srcRank[0];
            if (rank != 0) {
                int idx = PageRank.idx(targetNodeId, starts);
                nextScores[idx][targetNodeId - starts[idx]] += rank;
            }
            return true;
        }

        void prepareNextIteration(int[][] prevScores) {
            this.prevScores = prevScores;
        }

        private void subsequentSync() {
            synchronizeScores(combineScores());
            this.behavior = runs;
        }

        private int[] combineScores() {
            assert prevScores != null;
            assert prevScores.length >= 1;
            int[][] prevScores = this.prevScores;

            int length = prevScores.length;
            int[] allScores = prevScores[0];
            for (int i = 1; i < length; i++) {
                int[] scores = prevScores[i];
                for (int j = 0; j < scores.length; j++) {
                    allScores[j] += scores[j];
                    scores[j] = 0;
                }
            }

            return allScores;
        }

        private void synchronizeScores(int[] allScores) {
            double alpha = this.alpha;
            double dampingFactor = this.dampingFactor;
            double[] pageRank = this.pageRank;

            int length = allScores.length;
            for (int i = 0; i < length; i++) {
                int sum = allScores[i];
                pageRank[i] = alpha + dampingFactor * (sum / 100_000.0);
                allScores[i] = 0;
            }
        }

        private int calculateRank(int nodeId, int startNode) {
            int degree = degrees.degree(nodeId, Direction.OUTGOING);
            double rank = degree == 0 ? 0 : pageRank[nodeId - startNode] / degree;
            return (int) (100_000 * rank);
        }
    }

    private static abstract class DoubleArrayResult implements PageRankResult {

        private final IdMapping idMapping;

        protected DoubleArrayResult(IdMapping idMapping) {
            this.idMapping = idMapping;
        }

        @Override
        public final double score(final long nodeId) {
            return score((int) nodeId);
        }

        @Override
        public final AbstractExporter<PageRankResult> exporter(
                final GraphDatabaseAPI db,
                TerminationFlag terminationFlag,
                final Log log,
                final String writeProperty,
                final ExecutorService executorService,
                final int concurrency) {
            return new PageRankResultExporter(
                    db,
                    idMapping,
                    log,
                    writeProperty,
                    executorService)
                    .withConcurrency(concurrency);
        }
    }

    private static final class PartitionedPrimitiveDoubleArrayResult extends DoubleArrayResult {
        private final double[][] partitions;
        private final int[] starts;

        private PartitionedPrimitiveDoubleArrayResult(
                IdMapping idMapping,
                double[][] partitions,
                int[] starts) {
            super(idMapping);
            this.partitions = partitions;
            this.starts = starts;
        }

        @Override
        public double score(final int nodeId) {
            int idx = PageRank.idx(nodeId, starts);
            return partitions[idx][nodeId - starts[idx]];
        }

        @Override
        public long size() {
            long size = 0;
            for (double[] partition : partitions) {
                size += partition.length;
            }
            return size;
        }
    }

    private static final class PrimitiveDoubleArrayResult extends DoubleArrayResult {
        private final double[] result;

        private PrimitiveDoubleArrayResult(
                IdMapping idMapping,
                double[] result) {
            super(idMapping);
            this.result = result;
        }

        @Override
        public double score(final int nodeId) {
            return result[nodeId];
        }

        @Override
        public long size() {
            return result.length;
        }

        @Override
        public boolean hasFastToDoubleArray() {
            return true;
        }

        @Override
        public double[] toDoubleArray() {
            return result;
        }
    }
}
