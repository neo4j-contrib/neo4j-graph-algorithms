package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.traverse.ParallelLocalQueueBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

/**
 * Multistep ForwardBackward Coloring algorithm.
 *
 * The algorithm computes the (most likely) biggest SCC in a set of nodes
 * by intersecting the (descendant) set of reachable nodes using only
 * OUTGOING connections with its predecessor-set of reachable nodes using
 * only INCOMING relationships. Its intersection builds a SCC.
 *
 * @author mknblch
 */
public class MultiStepFWBW {

    private final Graph graph;
    private final ParallelLocalQueueBFS traverse;
    private int root;

    public MultiStepFWBW(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        traverse = new ParallelLocalQueueBFS(graph, executorService, concurrency);
    }

    public IntSet compute(IntSet nodes) {
        root = pivot(nodes);
        // D <- BFS( G(V,E(V)), v)
        final IntScatterSet descendant = new IntScatterSet();
        traverse.bfs(root, Direction.OUTGOING, nodes::contains, descendant::add)
                .awaitTermination();
        // ST <- BFS( G(V, E'(V)), v)
        final IntSet rootSCC = new IntScatterSet();
        traverse.reset()
                .bfs(root, Direction.INCOMING, descendant::contains, rootSCC::add)
                .awaitTermination();
        // SCC <- V & ST
        rootSCC.retainAll(descendant);
        return rootSCC;
    }

    /**
     * return the root id of the biggest SCC
     * @return root id of SCC
     */
    public int getRoot() {
        return root;
    }

    /**
     * find node with highest product of in- and out-degree
     * v E V for which Din(V) * Dout(V) is max
     *
     * @param set the nodeSet
     * @return the nodeId
     */
    private int pivot(IntSet set) {
        int product = 0;
        int pivot[] = {0};
        set.forEach((IntProcedure) node -> {
            final int p = graph.degree(node, Direction.OUTGOING) * graph.degree(node, Direction.INCOMING);
            if (p > product) {
                pivot[0] = node;
            }
        });
        return pivot[0];
    }
}
