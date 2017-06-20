package org.neo4j.graphalgo.core.utils.container;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public abstract class MSBFS {

    protected final Graph graph;
    private final ExecutorService executorService;
    private final int concurrency;

    protected IntConsumer nodeConsumer;

    public MSBFS(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
    }

    public MSBFS withNodeConsumer(IntConsumer nodeConsumer) {
        this.nodeConsumer = nodeConsumer;
        return this;
    }

    public abstract void compute(int startNodeId, Direction direction);

    public static class TopDownMS extends MSBFS {

        private final int nodeCount;

        private final int[] frontier;
        private final int[] next;
        private final int[] seen;


        public TopDownMS(Graph graph, ExecutorService executorService, int concurrency) {
            super(graph, executorService, concurrency);
            nodeCount = graph.nodeCount();
            frontier = new int[nodeCount];
            next = new int[nodeCount];
            seen = new int[nodeCount];
        }

        @Override
        public void compute(int startNodeId, Direction direction) {
            // phase 1
            graph.forEachNode(v -> {
                if (frontier[v] == 0) {
                    return true;
                }
                graph.forEachRelationship(v, direction, (sourceNodeId, targetNodeId, relationId) -> {
                    next[targetNodeId] |= frontier[v];
                    return true;
                });
                return true;
            });
            // phase 2
            graph.forEachNode(v -> {
                if (next[v] == 0) {
                    return true;
                }
                next[v] &= ~seen[v];
                seen[v] |= next[v];
                // TODO

                return true;
            });
        }
    }
}
