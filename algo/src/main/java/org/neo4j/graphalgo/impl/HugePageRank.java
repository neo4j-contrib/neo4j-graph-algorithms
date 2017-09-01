package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeDegrees;
import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.api.HugeNodeIterator;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.DoubleArray;
import org.neo4j.graphalgo.core.utils.paged.IntArray;
import org.neo4j.graphalgo.exporter.HugePageRankResultExporter;
import org.neo4j.graphalgo.exporter.PageRankResult;
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
public class HugePageRank extends Algorithm<HugePageRank> implements PageRankAlgorithm {

    private final ComputeSteps computeSteps;
    private HugeIdMapping idMapping;

    /**
     * Forces sequential use. If you want parallelism, prefer
     * {@link #HugePageRank(ExecutorService, int, int, HugeIdMapping, HugeNodeIterator, HugeRelationshipIterator, HugeDegrees, double)}
     */
    HugePageRank(
            HugeIdMapping idMapping,
            HugeNodeIterator nodeIterator,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
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
    HugePageRank(
            ExecutorService executor,
            int concurrency,
            int batchSize,
            HugeIdMapping idMapping,
            HugeNodeIterator nodeIterator,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
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
                idMapping.hugeNodeCount(),
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
    public HugePageRank compute(int iterations) {
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
            HugeIdMapping idMapping,
            HugeNodeIterator nodeIterator,
            HugeDegrees degrees) {
        long nodeCount = idMapping.hugeNodeCount();
        PrimitiveLongIterator nodes = nodeIterator.hugeNodeIterator();
        List<Partition> partitions = new ArrayList<>();
        long start = 0;
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
            HugeIdMapping idMapping,
            HugeDegrees degrees) {
        return Collections.singletonList(
                new Partition(
                        idMapping.hugeNodeCount(),
                        null,
                        degrees,
                        0,
                        -1
                )
        );
    }

    private ComputeSteps createComputeSteps(
            int concurrency,
            long nodeCount,
            double dampingFactor,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
            List<Partition> partitions,
            ExecutorService pool) {
        if (concurrency <= 0) {
            concurrency = partitions.size();
        }
        final int expectedParallelism = Math.min(
                concurrency,
                partitions.size());
        List<ComputeStep> computeSteps = new ArrayList<>(expectedParallelism);
        LongArrayList starts = new LongArrayList(expectedParallelism);
        LongArrayList lengths = new LongArrayList(expectedParallelism);
        int partitionsPerThread = ParallelUtil.threadSize(
                concurrency + 1,
                partitions.size());
        Iterator<Partition> parts = partitions.iterator();

        while (parts.hasNext()) {
            Partition partition = parts.next();
            long partitionCount = partition.nodeCount;
            long start = partition.startNode;
            for (int i = 1; i < partitionsPerThread && parts.hasNext(); i++) {
                partition = parts.next();
                partitionCount += partition.nodeCount;
            }

            DoubleArray partitionRank = DoubleArray.newArray(partitionCount);
            partitionRank.fill(1.0 / nodeCount);
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

        long[] startArray = starts.toArray();
        long[] lengthArray = lengths.toArray();
        for (ComputeStep computeStep : computeSteps) {
            computeStep.setStarts(startArray, lengthArray);
        }
        return new ComputeSteps(computeSteps, concurrency, pool);
    }

    private static int idx(long id, long[] ids) {
        int length = ids.length;

        int low = 0;
        int high = length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = ids[mid];

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
    public HugePageRank me() {
        return this;
    }

    @Override
    public HugePageRank release() {

        return this;
    }

    private static final class Partition {

        private final long startNode;
        private final long nodeCount;

        Partition(
                long allNodeCount,
                PrimitiveLongIterator nodes,
                HugeDegrees degrees,
                long startNode,
                int batchSize) {

            long nodeCount;
            long partitionSize = 0;
            if (batchSize > 0) {
                nodeCount = 0;
                while (partitionSize < batchSize && nodes.hasNext()) {
                    long nodeId = nodes.next();
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
        private final List<ComputeStep> steps;
        private final ExecutorService pool;
        private final IntArray[][] scores;
        private final int concurrency;

        private ComputeSteps(
                List<ComputeStep> steps,
                int concurrency,
                ExecutorService pool) {
            this.concurrency = concurrency;
            assert !steps.isEmpty();
            this.steps = steps;
            this.pool = pool;
            int stepSize = steps.size();
            scores = new IntArray[stepSize][];
            Arrays.setAll(scores, i -> new IntArray[stepSize]);
        }

        PageRankResult getPageRank() {
            ComputeStep firstStep = steps.get(0);
            if (steps.size() > 1) {
                DoubleArray[] results = new DoubleArray[steps.size()];
                int i = 0;
                for (ComputeStep step : steps) {
                    results[i++] = step.pageRank;
                }
                return new PartitionedDoubleArrayResult(
                        idMapping,
                        results,
                        firstStep.starts);
            } else {
                return new DoubleArrayResult(idMapping, firstStep.pageRank);
            }
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
            IntArray[][] scores = this.scores;
            int i;
            for (i = 0; i < stepSize; i++) {
                synchronizeScores(steps.get(i), i, scores);
            }
        }

        private void synchronizeScores(
                ComputeStep step,
                int idx,
                IntArray[][] scores) {
            step.prepareNextIteration(scores[idx]);
            IntArray[] nextScores = step.nextScores;
            for (int j = 0, len = nextScores.length; j < len; j++) {
                scores[j][idx] = nextScores[j];
            }
        }
    }

    private interface Behavior {
        void run();
    }


    private static final class ComputeStep implements Runnable, HugeRelationshipConsumer {

        private long[] starts;
        private final HugeRelationshipIterator relationshipIterator;
        private final HugeDegrees degrees;

        private final double alpha;
        private final double dampingFactor;

        private final DoubleArray pageRank;
        private IntArray[] nextScores;
        private IntArray[] prevScores;

        private final long startNode;
        private final long endNode;
        private final long nodeCount;

        private int[] srcRank = new int[1];
        private Behavior behavior;

        private Behavior runs = this::runsIteration;
        private Behavior syncs = this::subsequentSync;

        ComputeStep(
                double dampingFactor,
                HugeRelationshipIterator relationshipIterator,
                HugeDegrees degrees,
                DoubleArray pageRank,
                long startNode) {
            this.dampingFactor = dampingFactor;
            this.alpha = 1.0 - dampingFactor;
            this.relationshipIterator = relationshipIterator;
            this.degrees = degrees;
            this.startNode = startNode;
            this.nodeCount = pageRank.size();
            this.endNode = startNode + pageRank.size();
            this.pageRank = pageRank;
            this.behavior = runs;
        }

        void setStarts(long[] starts, long[] lengths) {
            this.starts = starts;
            this.nextScores = new IntArray[starts.length];
            Arrays.setAll(nextScores, i -> IntArray.newArray(lengths[i]));
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
            long startNode = this.startNode;
            long endNode = this.endNode;
            int[] srcRank = this.srcRank;
            HugeRelationshipIterator rels = this.relationshipIterator;
            for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
                int rank = calculateRank(nodeId, startNode);
                if (rank != 0) {
                    srcRank[0] = rank;
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }

        @Override
        public boolean accept(
                long sourceNodeId,
                long targetNodeId) {
            int rank = srcRank[0];
            if (rank != 0) {
                int idx = HugePageRank.idx(targetNodeId, starts);
                nextScores[idx].addTo(targetNodeId - starts[idx], rank);
            }
            return true;
        }

        void prepareNextIteration(IntArray[] prevScores) {
            this.prevScores = prevScores;
        }

        private void subsequentSync() {
            synchronizeScores(combineScores());
            this.behavior = runs;
        }

        private IntArray combineScores() {
            assert prevScores != null;
            assert prevScores.length >= 1;
            IntArray[] prevScores = this.prevScores;

            int length = prevScores.length;
            IntArray allScores = prevScores[0];

            for (int i = 1; i < length; i++) {
                IntArray scores = prevScores[i];
                IntArray.Cursor cursor = scores.cursorForAll();
                while (cursor.next()) {
                    int[] array = cursor.array;
                    int end = cursor.limit;
                    for (int j = cursor.offset; i < end; i++) {
                        allScores.addTo(j, array[j]);
                        array[j] = 0;
                    }
                }
                scores.returnCursor(cursor);
            }

            return allScores;
        }

        private void synchronizeScores(IntArray allScores) {
            double alpha = this.alpha;
            double dampingFactor = this.dampingFactor;
            DoubleArray pageRank = this.pageRank;

            IntArray.Cursor cursor = allScores.cursorForAll();
            while (cursor.next()) {
                int[] array = cursor.array;
                int end = cursor.limit;
                for (int i = cursor.offset; i < end; i++) {
                    int sum = array[i];
                    pageRank.set(i, alpha + dampingFactor * (sum / 100_000.0));
                    array[i] = 0;
                }
            }
            allScores.returnCursor(cursor);
        }

        private int calculateRank(long nodeId, long startNode) {
            int degree = degrees.degree(nodeId, Direction.OUTGOING);
            double rank = degree == 0 ? 0 : pageRank.get(nodeId - startNode) / degree;
            return (int) (100_000 * rank);
        }
    }

    private static abstract class HugeResult implements PageRankResult {
        private final HugeIdMapping idMapping;

        protected HugeResult(HugeIdMapping idMapping) {
            this.idMapping = idMapping;
        }

        @Override
        public final double score(final int nodeId) {
            return score((long) nodeId);
        }

        @Override
        public final AbstractExporter<PageRankResult> exporter(
                final GraphDatabaseAPI db,
                TerminationFlag terminationFlag,
                final Log log,
                final String writeProperty,
                final ExecutorService executorService,
                final int concurrency) {
            return new HugePageRankResultExporter(
                    db,
                    terminationFlag,
                    idMapping,
                    log,
                    writeProperty,
                    executorService)
                    .withConcurrency(concurrency);
        }
    }

    private static final class PartitionedDoubleArrayResult extends HugeResult {
        private final DoubleArray[] partitions;
        private final long[] starts;

        private PartitionedDoubleArrayResult(
                HugeIdMapping idMapping,
                DoubleArray[] partitions,
                long[] starts) {
            super(idMapping);
            this.partitions = partitions;
            this.starts = starts;
        }

        @Override
        public double score(final long nodeId) {
            int idx = HugePageRank.idx(nodeId, starts);
            return partitions[idx].get(nodeId - starts[idx]);
        }

        @Override
        public long size() {
            long size = 0;
            for (DoubleArray partition : partitions) {
                size += partition.size();
            }
            return size;
        }
    }

    private static final class DoubleArrayResult extends HugeResult {
        private final DoubleArray result;

        private DoubleArrayResult(
                HugeIdMapping idMapping,
                DoubleArray result) {
            super(idMapping);
            this.result = result;
        }

        @Override
        public final double score(final long nodeId) {
            return result.get(nodeId);
        }

        @Override
        public long size() {
            return result.size();
        }
    }
}
