package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Multi Source Breadth First Search implemented as described in [1].
 * <p>
 * The benefit of running this MS-BFS instead of multiple execution of a regular
 * BFS for every source is that the MS-BFS algorithm can collapse traversals that are
 * the same for multiple nodes. If any two or more given BFSs would traverse the same nodes
 * at the same iteration depth, the MS-BFS will traverse only once and group all sources
 * for this traversal.
 * <p>
 * The consumer of this algorithm provides a callback function, the gets called
 * with:
 * <ul>
 * <li>the node id where the BFS traversal is at</li>
 * <li>the depth or BFS iteration at which this node is traversed</li>
 * <li>a lazily evaluated list of all source nodes that have arrived at this node at the same depth/iteration</li>
 * </ul>
 * The sources iterator is only valid during the execution of the callback and
 * should not be stored.
 * <p>
 * We use a fixed {@code ω} (OMEGA) of 32, which allows us to implement the
 * seen/visitNext bit sets as a packed long which improves memory locality
 * as suggested in 4.1. of the paper.
 * If the number of sources exceed 32, multiple instances of MS-BFS are run
 * in parallel.
 * <p>
 * If the MS-BFS runs in parallel, the callback may be executed from multiple threads
 * at the same time. The implementation should therefore be thread-safe.
 * <p>
 * The algorithm provides two invariants:
 * <ul>
 * <li>
 * For a single thread, a single node is traversed at most once at a given depth
 * – That is, the combination of {@code (nodeId, depth)} appears at most once per thread.
 * It may be that a node is traversed multiple times, but then always at different depths.
 * </li>
 * <li>
 * For multiple threads, the {@code (nodeId, depth)} may appear multiple times, but then always
 * for different sources.
 * </li>
 * </ul>
 * <p>
 * [1]: <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">The More the Merrier: Efficient Multi-Source Graph Traversal</a>
 */
public final class HugeMultiSourceBFS implements Runnable, MsBFSAlgo {

    // how many sources can be traversed simultaneously
    static final int OMEGA = 32;

    private final ThreadLocal<HugeMultiBitSet32> visits;
    private final ThreadLocal<HugeBiMultiBitSet32> nextAndSeens;

    private final HugeIdMapping nodeIds;
    private final HugeRelationshipIterator relationships;
    private final Direction direction;
    private final HugeBfsConsumer perNodeAction;
    private final long[] startNodes;
    private long nodeOffset, nodeCount;
    private int sourceNodeCount;

    public HugeMultiSourceBFS(
            HugeIdMapping nodeIds,
            HugeRelationshipIterator relationships,
            Direction direction,
            HugeBfsConsumer perNodeAction,
            AllocationTracker tracker,
            long... startNodes) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = (startNodes != null && startNodes.length > 0) ? startNodes : null;
        if (this.startNodes != null) {
            Arrays.sort(this.startNodes);
        }
        nodeCount = nodeIds.nodeCount();
        this.visits = new VisitLocal(nodeCount, tracker);
        this.nextAndSeens = new NextAndSeenLocal(nodeCount, tracker);
    }

    private HugeMultiSourceBFS(
            HugeIdMapping nodeIds,
            HugeRelationshipIterator relationships,
            Direction direction,
            HugeBfsConsumer perNodeAction,
            ThreadLocal<HugeMultiBitSet32> visits,
            ThreadLocal<HugeBiMultiBitSet32> nextAndSeens,
            long... startNodes) {
        assert startNodes != null && startNodes.length > 0;
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = startNodes;
        this.visits = visits;
        this.nextAndSeens = nextAndSeens;
    }

    private HugeMultiSourceBFS(
            HugeIdMapping nodeIds,
            HugeRelationshipIterator relationships,
            Direction direction,
            HugeBfsConsumer perNodeAction,
            long nodeOffset,
            int sourceNodeCount,
            ThreadLocal<HugeMultiBitSet32> visits,
            ThreadLocal<HugeBiMultiBitSet32> nextAndSeens) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = null;
        this.nodeOffset = nodeOffset;
        this.sourceNodeCount = sourceNodeCount;
        this.visits = visits;
        this.nextAndSeens = nextAndSeens;
    }

    /**
     * Runs MS-BFS, possibly in parallel.
     */
    @Override
    public void run(int concurrency, ExecutorService executor) {
        final int threads = numberOfThreads();
        Collection<HugeMultiSourceBFS> bfss = allSourceBfss(threads);
        if (!ParallelUtil.canRunInParallel(executor)) {
            // fallback to sequentially running all MS-BFS instances
            executor = null;
        }
        ParallelUtil.runWithConcurrency(
                concurrency,
                bfss,
                threads << 2,
                100L,
                TimeUnit.MICROSECONDS,
                executor);
    }

    /**
     * Runs MS-BFS, always single-threaded. Requires that there are at most
     * 32 startNodes. If there are more, {@link #run(int, ExecutorService)} must be used.
     */
    @Override
    public void run() {
        assert sourceLength() <= OMEGA : "more than " + OMEGA + " sources not supported";

        SourceNodes sourceNodes = startNodes != null
                ? new SourceNodes(startNodes)
                : new SourceNodes(nodeOffset, sourceNodeCount);

        HugeMultiBitSet32 visit = visits.get();
        HugeBiMultiBitSet32 nextAndSeen = nextAndSeens.get();

        if (startNodes != null) {
            nextAndSeen.setAuxBits(startNodes);
            for (int i = 0; i < startNodes.length; i++) {
                visit.setBit(startNodes[i], i);
            }
        } else {
            nextAndSeen.setAuxBits(nodeOffset, sourceNodeCount);
            for (int i = 0; i < sourceNodeCount; i++) {
                visit.setBit(i + nodeOffset, i);
            }
        }

        long depth = 0;

        while (true) {
            long nodeId = -1L;
            while ((nodeId = visit.nextSetNodeId(nodeId + 1)) >= 0) {
                int nodeVisit = visit.get(nodeId);
                assert nodeVisit != 0;
                relationships.forEachRelationship(
                        nodeId,
                        direction,
                        (src, tgt) -> {
                            nextAndSeen.union(tgt, nodeVisit);
                            return true;
                        });
            }

            depth++;
            nodeId = -1L;
            // TODO: implement Direction-Optimized Traversal (4.1.2.)
            while ((nodeId = nextAndSeen.nextSetNodeId(nodeId + 1)) >= 0) {
                int D = nextAndSeen.unionDifference(nodeId);
                if (D != 0) {
                    sourceNodes.reset(D);
                    perNodeAction.accept(nodeId, depth, sourceNodes);
                }
            }

            if (nodeId == -2 || !nextAndSeen.copyInto(visit)) {
                // nothing more to visit, stop bfs
                return;
            }
        }
    }

    private long sourceLength() {
        if (startNodes != null) {
            return startNodes.length;
        }
        if (sourceNodeCount == 0) {
            return nodeCount;
        }
        return sourceNodeCount;
    }

    private int numberOfThreads() {
        long sourceLength = sourceLength();
        long threads = ParallelUtil.threadSize(OMEGA, sourceLength);
        if ((int) threads != threads) {
            throw new IllegalArgumentException("Unable run MS-BFS on " + sourceLength + " sources.");
        }
        return (int) threads;
    }

    // lazily creates MS-BFS instances for OMEGA sized source chunks
    private Collection<HugeMultiSourceBFS> allSourceBfss(int threads) {
        if (startNodes == null) {
            long sourceLength = nodeCount;
            return new ParallelMultiSources(threads, sourceLength) {
                @Override
                HugeMultiSourceBFS next(final long from, final int length) {
                    return new HugeMultiSourceBFS(
                            nodeIds,
                            relationships.concurrentCopy(),
                            direction,
                            perNodeAction,
                            from,
                            length,
                            visits,
                            nextAndSeens
                    );
                }
            };
        }
        long[] startNodes = this.startNodes;
        int sourceLength = startNodes.length;
        return new ParallelMultiSources(threads, sourceLength) {
            @Override
            HugeMultiSourceBFS next(final long from, final int length) {
                return new HugeMultiSourceBFS(
                        nodeIds,
                        relationships.concurrentCopy(),
                        direction,
                        perNodeAction,
                        visits,
                        nextAndSeens,
                        Arrays.copyOfRange(startNodes, (int) from, (int) (from + length))
                );
            }
        };
    }

    @Override
    public String toString() {
        if (startNodes != null && startNodes.length > 0) {
            return "MSBFS{" + startNodes[0] +
                    " .. " + (startNodes[startNodes.length - 1] + 1) +
                    " (" + startNodes.length +
                    ")}";
        }
        return "MSBFS{" + nodeOffset +
                " .. " + (nodeOffset + sourceNodeCount) +
                " (" + sourceNodeCount +
                ")}";
    }

    private static final class SourceNodes implements HugeBfsSources {
        private final long[] sourceNodes;
        private final int maxPos;
        private final int startPos;
        private final long offset;
        private int sourceMask;
        private int pos;

        private SourceNodes(long[] sourceNodes) {
            assert sourceNodes.length <= OMEGA;
            this.sourceNodes = sourceNodes;
            this.maxPos = sourceNodes.length;
            this.offset = 0L;
            this.startPos = -1;
        }

        private SourceNodes(long offset, int length) {
            assert length <= OMEGA;
            this.sourceNodes = null;
            this.maxPos = length;
            this.offset = offset;
            this.startPos = -1;
        }

        public void reset() {
            this.pos = startPos;
            fetchNext();
        }

        void reset(int sourceMask) {
            this.sourceMask = sourceMask;
            reset();
        }

        @Override
        public boolean hasNext() {
            return pos < maxPos;
        }

        @Override
        public long next() {
            int current = this.pos;
            fetchNext();
            return sourceNodes != null ? sourceNodes[current] : current + offset;
        }

        @Override
        public int size() {
            return Integer.bitCount(sourceMask);
        }

        private void fetchNext() {
            //noinspection StatementWithEmptyBody
            while (++pos < maxPos && (sourceMask & (1 << pos)) == 0)
                ;
        }
    }

    private static abstract class ParallelMultiSources extends AbstractCollection<HugeMultiSourceBFS> implements Iterator<HugeMultiSourceBFS> {
        private final int threads;
        private final long sourceLength;
        private long start = 0L;
        private int i = 0;

        private ParallelMultiSources(int threads, long sourceLength) {
            this.threads = threads;
            this.sourceLength = sourceLength;
        }

        @Override
        public boolean hasNext() {
            return i < threads;
        }

        @Override
        public int size() {
            return threads;
        }

        @Override
        public Iterator<HugeMultiSourceBFS> iterator() {
            start = 0L;
            i = 0;
            return this;
        }

        @Override
        public HugeMultiSourceBFS next() {
            int len = (int) Math.min(OMEGA, sourceLength - start);
            HugeMultiSourceBFS bfs = next(start, len);
            start += len;
            i++;
            return bfs;
        }

        abstract HugeMultiSourceBFS next(long from, int length);
    }

    private static final class VisitLocal extends ThreadLocal<HugeMultiBitSet32> {
        private final long nodeCount;
        private final AllocationTracker tracker;

        private VisitLocal(
                long nodeCount,
                AllocationTracker tracker) {
            this.nodeCount = nodeCount;
            this.tracker = tracker;
        }

        @Override
        protected HugeMultiBitSet32 initialValue() {
            return new HugeMultiBitSet32(nodeCount, tracker);
        }
    }

    private static final class NextAndSeenLocal extends ThreadLocal<HugeBiMultiBitSet32> {
        private final long nodeCount;
        private final AllocationTracker tracker;

        private NextAndSeenLocal(
                long nodeCount,
                AllocationTracker tracker) {
            this.nodeCount = nodeCount;
            this.tracker = tracker;
        }

        @Override
        protected HugeBiMultiBitSet32 initialValue() {
            return new HugeBiMultiBitSet32(nodeCount, tracker);
        }
    }
}
