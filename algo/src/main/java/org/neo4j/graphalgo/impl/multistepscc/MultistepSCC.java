package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mknblch
 */
public class MultistepSCC {

    private final ParallelTraverse traverse;
    private final MultiStepColoring coloring;
    private final Graph graph;
    private final int cutOff;
    private final MultiStepTrim trimming;
    private final MultiStepFWBW fwbw;

    private final IntObjectMap<IntSet> connectedComponents;

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

    public Stream<SCCStreamResult> resultStream() {
        return StreamSupport.stream(connectedComponents.spliterator(), false)
                .flatMap(mapCursor -> StreamSupport.stream(mapCursor.value.spliterator(), false)
                        .map(setCursor -> new SCCStreamResult(graph.toOriginalNodeId(setCursor.value), mapCursor.key)));
    }

    public IntObjectMap<IntSet> getConnectedComponents() {
        return connectedComponents;
    }

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
