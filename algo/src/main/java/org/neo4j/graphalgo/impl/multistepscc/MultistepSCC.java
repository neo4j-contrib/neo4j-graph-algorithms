package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Multistep: parallel strongly connected component algorithm
 *
 * The algorithm consists of multiple steps to calculate strongly connected components.
 *
 * The algo. starts by trimming all nodes without either incoming or outgoing
 * relationships to minimize the search vector. It then does a Forward-Backward coloring
 * which first determines the starting point by evaluating the highest product of in- and
 * out- degree in the graph. Starting from this point the algorithm colors all reachable
 * nodes (with outgoing relationships) with their highest node-id as color. The resulting
 * node-set is called the descendant-set. It then calculates a set of nodes by collecting
 * all reachable nodes using incoming relationships, called the predecessor set. The
 * intersection of both sets builds a strongly connected component. With high probability
 * the biggest SCC in the Graph.
 *
 * After finding the biggest SCC it removes its nodes from the original nodeSet which
 * results in the rest-set. The set is then used by the coloring algorithm which
 * extracts one weakly connected component set each time. the main loop builds its predecessor
 * set and the intersection of both (which is also an SCC). After removing the the SCC from
 * the nodeSet the algorithm continues with the next color/scc-element until the nodeCount
 * falls under a threshold. Sequential Tarjan algorithm is then used to extract remaining
 * SCCs of the nodeSet until no more set can be build.
 *
 * @author mknblch
 */
public class MultistepSCC {

    // the graph
    private final Graph graph;
    // parallel BFS impl.
    private final ParallelTraverse traverse;
    // parallel multistep coloring algo
    private final MultiStepColoring coloring;
    // cutoff value (threshold for sequential tarjan)
    private final int cutOff;
    // trimming algorithm
    private final MultiStepTrim trimming;
    // forward backward coloring algorithm
    private final MultiStepFWBW fwbw;
    // map rootNode -> {set of strongly connected node ID's}
    private final IntObjectMap<IntSet> connectedComponents;
    // sequential tarjan algorithm
    private final AbstractMultiStepTarjan tarjan;

    public MultistepSCC(Graph graph, ExecutorService executorService, int concurrency, int cutOff) {
        this.graph = graph;
        this.cutOff = cutOff;
        trimming = new MultiStepTrim(graph);
        coloring = new MultiStepColoring(graph, executorService, concurrency);
        fwbw = new MultiStepFWBW(graph, executorService, concurrency);
        traverse = new ParallelTraverse(graph, executorService, concurrency);
        tarjan = new AbstractMultiStepTarjan(graph) {
            @Override
            public void processSCC(int root, IntHashSet connected) {
                MultistepSCC.this.processSCC(root, connected);
            }
        };
        connectedComponents = new IntObjectScatterMap<>();
    }

    public MultistepSCC compute() {
        // V <- simpleTrim (V)
        final IntSet nodeSet = trimming.compute(false);
        final IntSet rootSCC = fwbw.compute(nodeSet);
        // rootSCC should be biggest SCC
        processSCC(fwbw.getRoot(), rootSCC);
        // V <- V \ SCC
        nodeSet.removeAll((IntLookupContainer) rootSCC);
        // compute colors of the resulting node set
        coloring.compute(nodeSet);
        final AtomicIntegerArray colors = coloring.getColors();
        // backward coloring until cutoff threshold is reached
        coloring.forEachColor(color -> {
            // SCC(cv) <- PREDECESSOR( V(cv), c)
            final IntSet scc = pred(nodeSet, colors, color);
            processSCC(color, scc);
            // V <- V \ SCCc
            nodeSet.removeAll((IntLookupContainer) scc);
            // check threshold
            return nodeSet.size() > cutOff;
        });
        // nodeSet size below threshold, do sequential tarjan
        tarjan.compute(nodeSet);
        return this;
    }

    /**
     * return the result stream
     * @return stream of result DTOs
     */
    public Stream<SCCStreamResult> resultStream() {
        return StreamSupport.stream(connectedComponents.spliterator(), false)
                .flatMap(mapCursor -> StreamSupport.stream(mapCursor.value.spliterator(), false)
                        .map(setCursor -> new SCCStreamResult(graph.toOriginalNodeId(setCursor.value), mapCursor.key)));
    }

    /**
     * get the whole map of connected components
     * @return
     */
    public IntObjectMap<IntSet> getConnectedComponents() {
        return connectedComponents;
    }

    /**
     * process a SCC if found (may be empty)
     * @param root
     * @param elements
     */
    private void processSCC(int root, IntSet elements) {
        if (elements.isEmpty()) {
            return;
        }
        connectedComponents.put(root, elements);
    }

    /**
     * traverse backwards and collect all connected nodes with the same color
     * as the start node id ( start color )
     * @param nodes
     * @param cv denotes the startNodeId and the color
     * @return set with all nodes reachable backwards with the same color as the startNode (is an SCC)
     */
    private IntSet pred(final IntSet nodes, AtomicIntegerArray colors, final int cv) {
        final IntScatterSet set = new IntScatterSet();
        traverse.reset()
                .bfs(cv, Direction.INCOMING, nodes::contains, node -> {
                    if (colors.get(node) == cv) {
                        set.add(node);
                    }
                })
                .awaitTermination();

        return set;
    }


    public static class SCCStreamResult {

        /**
         * the node id
         */
        public final long nodeId;

        /**
         * the set id of the stronly connected component
         */
        public final long clusterId;

        public SCCStreamResult(long nodeId, int clusterId) {
            this.nodeId = nodeId;
            this.clusterId = clusterId;
        }
    }
}
