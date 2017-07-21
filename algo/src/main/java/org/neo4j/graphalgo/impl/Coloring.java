package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.ParallelLocalQueueBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Coloring is a (singl esource) parallel connected
 * Components algorithm based on ParallelLocalQueueBFS
 *
 * @author mknblch
 */
public class Coloring extends Algorithm<Coloring> {

    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final int concurrency;

    public Coloring(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        colors = new AtomicIntegerArray(graph.nodeCount());
        this.concurrency = concurrency;
    }

    public AtomicIntegerArray getColors() {
        return colors;
    }

    public Coloring compute() {
        // reset state so that each node has its own id as color
        reset();
        // create single source parallel bfs
        final ParallelLocalQueueBFS bfs = new ParallelLocalQueueBFS(
                graph,
                executorService,
                concurrency);
        // for each node assign color
        graph.forEachNode(source -> {
            final int sourceColor = colors.get(source);
            bfs.bfs(source, Direction.OUTGOING, i -> running(), target -> {
                final int targetColor = colors.get(target);
                if (sourceColor > targetColor) {
                    setColor(target, sourceColor);
                } else if (targetColor > sourceColor) {
                    setColor(source, targetColor);
                }
            });
            return true;
        });
        // wait until all bfs finished
        bfs.awaitTermination();
        return this;
    }

    public int getSetCount() {
        final IntIntMap map = new IntIntScatterMap();
        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            int color = colors.get(i);
            map.addTo(color, 1);
        }
        return map.size();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), colors.get(i)));
    }

    @Override
    public Coloring me() {
        return this;
    }

    private void reset() {
        ParallelUtil.iterateParallel(executorService,
                graph.nodeCount(),
                concurrency,
                offset -> colors.set(offset, offset));
    }

    private void setColor(int node, int color) {
        /* loop until either current is higher or equal
           to color or color was successfully saved */
        int current;
        do {
            current = colors.get(node);
        } while (color >= current && !colors.compareAndSet(node, current, color));
    }

    public static class Result {

        public final long nodeId;

        public final long color;

        public Result(long nodeId, int color) {
            this.nodeId = nodeId;
            this.color = color;
        }
    }
}
