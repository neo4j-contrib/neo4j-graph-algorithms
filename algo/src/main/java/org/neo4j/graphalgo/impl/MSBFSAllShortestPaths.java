package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * AllShortestPaths:
 * <p>
 * multi-source parallel dijkstra algorithm for computing the shortest path between
 * each pair of nodes.
 * <p>
 * Since all nodeId's have already been ordered by the idMapping we can use an integer
 * instead of a queue which just count's up for each startNodeId as long as it is
 * < nodeCount. Each thread tries to take one int from the counter at one time and
 * starts its computation on it.
 * <p>
 * The {@link MSBFSAllShortestPaths} value determines the count of workers
 * that should be spawned.
 * <p>
 * Due to the high memory footprint the result set would have we emit each result into
 * a blocking queue. The result stream takes elements from the queue while the workers
 * add elements to it. The result stream is limited by N^2. If the stream gets closed
 * prematurely the workers get closed too.
 */
public class MSBFSAllShortestPaths extends Algorithm<MSBFSAllShortestPaths> {

    private final Graph graph;
    private final ExecutorService executorService;
    private final BlockingQueue<Result> resultQueue;
    private final int nodeCount;

    public MSBFSAllShortestPaths(Graph graph, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.resultQueue = new LinkedBlockingQueue<>(); // TODO limit size?
    }

    /**
     * the resultStream(..) method starts the computation and
     * returns a Stream of SP-Tuples (source, target, minDist)
     *
     * @return the result stream
     */
    public Stream<Result> resultStream() {
        executorService.submit(new ShortestPathTask(executorService));
        Iterator<Result> iterator = new AbstractIterator<Result>() {
            @Override
            protected Result fetch() {
                try {
                    Result result = resultQueue.take();
                    if (result.sourceNodeId == -1) {
                        return done();
                    }
                    return result;
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iterator,
                0), false);
    }

    @Override
    public MSBFSAllShortestPaths me() {
        return this;
    }

    /**
     * Dijkstra Task. Takes one element of the counter at a time
     * and starts dijkstra on it. It starts emitting results to the
     * queue once all reachable nodes have been visited.
     */
    private class ShortestPathTask implements Runnable {

        private final ExecutorService executorService;

        private ShortestPathTask(final ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void run() {

            final ProgressLogger progressLogger = getProgressLogger();

            new MultiSourceBFS(
                    graph,
                    graph,
                    Direction.OUTGOING,
                    (target, distance, sources) -> {
                        while (sources.hasNext()) {
                            int source = sources.next();
                            final Result result = new Result(
                                    graph.toOriginalNodeId(source),
                                    graph.toOriginalNodeId(target),
                                    distance);

                            try {
                                resultQueue.put(result);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        progressLogger.logProgress((double) target / (nodeCount - 1));
                    }
            ).run(executorService);

            resultQueue.add(new Result(-1, -1, -1));
        }
    }

    /**
     * Result DTO
     */
    public static class Result {

        /**
         * neo4j nodeId of the source node
         */
        public final long sourceNodeId;
        /**
         * neo4j nodeId of the target node
         */
        public final long targetNodeId;
        /**
         * minimum distance between source and target
         */
        public final double distance;

        public Result(long sourceNodeId, long targetNodeId, double distance) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "sourceNodeId=" + sourceNodeId +
                    ", targetNodeId=" + targetNodeId +
                    ", distance=" + distance +
                    '}';
        }
    }
}
