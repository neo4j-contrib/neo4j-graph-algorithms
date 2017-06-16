package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class MultistepSCC {

    private final Traverse traverse;
    private final MultiStepColoring coloring;
    private final int concurrency;
    private final int cutOff;
    private final int nodeCount;
    private final MultiStepTrim trimming;
    private final MultiStepFWBW fwbw;

    private final ObjectArrayList<IntSet> connectedComponents;
    private final MultiStepTarjan tarjan;

    public MultistepSCC(Graph graph, ExecutorService executorService, int concurrency, int cutOff) {

        this.concurrency = concurrency;
        this.cutOff = cutOff;
        this.nodeCount = graph.nodeCount();

        connectedComponents = new ObjectArrayList<IntSet>();

        traverse = new Traverse(graph);
        trimming = new MultiStepTrim(graph);
        coloring = new MultiStepColoring(graph, executorService, concurrency);
        fwbw = new MultiStepFWBW(graph);
        tarjan = new MultiStepTarjan(graph, connectedComponents);

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

    public ObjectArrayList<IntSet> getConnectedComponents() {
        return connectedComponents;
    }

    private void processSCC(int root, IntSet elements) {
        if (elements.isEmpty()) {
            return;
        }
        connectedComponents.add(elements);
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
        traverse.bfs(cv, Direction.INCOMING, nodes::contains, node -> {
            if (colors.get(node) == cv) {
                set.add(node);
            }
        });
        return set;
    }
}
