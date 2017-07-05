package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

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
public final class MultiSourceBFS implements Runnable {

    // how many sources can be traversed simultaneously
    private static final int OMEGA = 32;

    private final ThreadLocal<MultiBitSet32> visits;
    private final ThreadLocal<BiMultiBitSet32> nextAndSeens;

    private final IdMapping nodeIds;
    private final RelationshipIterator relationships;
    private final Direction direction;
    private final BfsConsumer perNodeAction;
    private final int[] startNodes;
    private int nodeOffset, sourceNodeCount;

    public MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            int... startNodes) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = (startNodes != null && startNodes.length > 0) ? startNodes : null;
        this.visits = new VisitLocal(nodeIds.nodeCount());
        this.nextAndSeens = new NextAndSeenLocal(nodeIds.nodeCount());
    }

    private MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            ThreadLocal<MultiBitSet32> visits,
            ThreadLocal<BiMultiBitSet32> nextAndSeens,
            int... startNodes) {
        assert startNodes != null && startNodes.length > 0;
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = startNodes;
        this.visits = visits;
        this.nextAndSeens = nextAndSeens;
    }

    private MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            int nodeOffset,
            int sourceNodeCount,
            ThreadLocal<MultiBitSet32> visits,
            ThreadLocal<BiMultiBitSet32> nextAndSeens) {
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
    public void run(ExecutorService executor) {
        int sourceLength = sourceLength();
        int threads = ParallelUtil.threadSize(OMEGA, sourceLength);
        if (threads > 1 && ParallelUtil.canRunInParallel(executor)) {
            runParallel(executor, threads);
        } else {
            if (sourceLength > OMEGA) {
                // TODO support running multiple chunks sequentially
                throw new IllegalArgumentException("In order to run MS-BFS on " + sourceLength + " sources, you have to provide a valid ExecutorService");
            }
            if (startNodes == null) {
                nodeOffset = 0;
                sourceNodeCount = nodeIds.nodeCount();
            }
            run();
        }
    }

    /**
     * Runs MS-BFS, always single-threaded. Requires that there are at most
     * 32 startNodes. If there are more, {@link #run(ExecutorService)} must be used.
     */
    @Override
    public void run() {
        assert sourceLength() <= OMEGA : "more than " + OMEGA + " sources not supported";

        SourceNodes sourceNodes = startNodes != null
                ? new SourceNodes(startNodes)
                : new SourceNodes(nodeOffset, sourceNodeCount);

        MultiBitSet32 visit = visits.get();
        BiMultiBitSet32 nextAndSeen = nextAndSeens.get();

        if (startNodes != null) {
            for (int i = 0; i < startNodes.length; i++) {
                nextAndSeen.setAuxBit(startNodes[i], i);
                visit.setBit(startNodes[i], i);
            }
        } else {
            for (int i = 0; i < sourceNodeCount; i++) {
                nextAndSeen.setAuxBit(i + nodeOffset, i);
                visit.setBit(i + nodeOffset, i);
            }
        }

        int depth = 0;

        while (true) {
            int nodeId = -1;
            while ((nodeId = visit.nextSetNodeId(nodeId + 1)) >= 0) {
                int nodeVisit = visit.get(nodeId);
                assert nodeVisit != 0;
                relationships.forEachRelationship(
                        nodeId,
                        direction,
                        (src, tgt, rel) -> {
                            nextAndSeen.union(tgt, nodeVisit);
                            return true;
                        });
            }

            if (nodeId == -2) {
                // nothing more to visit, stop bfs
                nextAndSeen.reset();
                return;
            }

            depth++;
            nodeId = -1;
            // TODO: implement Direction-Optimized Traversal (4.1.2.)
            while ((nodeId = nextAndSeen.nextSetNodeId(nodeId + 1)) >= 0) {
                int D = nextAndSeen.unionDifference(nodeId);
                if (D != 0) {
                    sourceNodes.reset(D);
                    perNodeAction.accept(nodeId, depth, sourceNodes);
                }
            }

            nextAndSeen.copyInto(visit);
        }
    }

    private int sourceLength() {
        if (startNodes != null) {
            return startNodes.length;
        }
        if (sourceNodeCount == 0) {
            return nodeIds.nodeCount();
        }
        return sourceNodeCount;
    }

    private void runParallel(ExecutorService executor, int threads) {
        MultiSourceBFS[] bfss;
        if (startNodes == null) {
            int sourceLength = nodeIds.nodeCount();
            int start = 0;
            bfss = new MultiSourceBFS[threads];
            for (int i = 0; i < threads; i++) {
                int len = Math.min(OMEGA, sourceLength - start);
                bfss[i] = new MultiSourceBFS(
                        nodeIds,
                        relationships,
                        direction,
                        perNodeAction,
                        start,
                        len,
                        visits,
                        nextAndSeens
                );
                start += len;
            }
        } else {
            int[] startNodes = this.startNodes;
            int sourceLength = startNodes.length;
            int start = 0;
            bfss = new MultiSourceBFS[threads];
            for (int i = 0; i < threads; i++) {
                int to = Math.min(sourceLength, start + OMEGA);
                bfss[i] = new MultiSourceBFS(
                        nodeIds,
                        relationships,
                        direction,
                        perNodeAction,
                        visits,
                        nextAndSeens,
                        Arrays.copyOfRange(startNodes, start, to)
                );
            }
        }

        ParallelUtil.run(Arrays.asList(bfss), executor);
    }


    private static final class SourceNodes implements PrimitiveIntIterator {
        private final int[] sourceNodes;
        private final int maxPos;
        private final int startPos;
        private final int offset;
        private int sourceMask;
        private int pos;

        private SourceNodes(int[] sourceNodes) {
            assert sourceNodes.length <= OMEGA;
            this.sourceNodes = sourceNodes;
            this.maxPos = sourceNodes.length;
            this.offset = 0;
            this.startPos = -1;
        }

        private SourceNodes(int offset, int length) {
            assert length <= OMEGA;
            this.sourceNodes = null;
            this.maxPos = length;
            this.offset = offset;
            this.startPos = -1;
        }

        void reset(int sourceMask) {
            this.sourceMask = sourceMask;
            this.pos = startPos;
            fetchNext();
        }

        @Override
        public boolean hasNext() {
            return pos < maxPos;
        }

        @Override
        public int next() {
            int current = this.pos;
            fetchNext();
            return sourceNodes != null ? sourceNodes[current] : current + offset;
        }

        private void fetchNext() {
            //noinspection StatementWithEmptyBody
            while (++pos < maxPos && (sourceMask & (1 << pos)) == 0)
                ;
        }
    }

    private static final class VisitLocal extends ThreadLocal<MultiBitSet32> {
        private final int nodeCount;

        private VisitLocal(final int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        protected MultiBitSet32 initialValue() {
            return new MultiBitSet32(nodeCount);
        }
    }

    private static final class NextAndSeenLocal extends ThreadLocal<BiMultiBitSet32> {
        private final int nodeCount;

        private NextAndSeenLocal(final int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        protected BiMultiBitSet32 initialValue() {
            return new BiMultiBitSet32(nodeCount);
        }
    }
}
