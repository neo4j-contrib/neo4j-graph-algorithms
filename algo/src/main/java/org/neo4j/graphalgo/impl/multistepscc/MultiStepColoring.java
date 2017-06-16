package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphalgo.core.utils.container.FlipStack;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntPredicate;

/**
 * Multistep SCC coloring algorithm
 *
 * http://www.sandia.gov/~srajama/publications/BFS_and_Coloring.pdf
 * https://www.osti.gov/scitech/servlets/purl/1115145
 *
 * @author mknblch
 */
public class MultiStepColoring {

    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final AtomicBitSet visited;
    private final List<Future<IntContainer>> futures = new ArrayList<>();
    private final int concurrency;
    private final int nodeCount;

    public MultiStepColoring(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.concurrency = concurrency;
        colors = new AtomicIntegerArray(nodeCount);
        visited = new AtomicBitSet(nodeCount);
    }

    /**
     * compute the
     * @param nodes
     * @return self for method chaining
     * */
    public MultiStepColoring compute(IntSet nodes) {

        resetColors(); // TODO ctor?
        msColorParallel(nodes);
        printColor();

        return this;
    }

    public AtomicIntegerArray getColors() {
        return colors;
    }

    /**
     * retrieve colorset
     * @return a set of distinct color id's
     */
    public IntScatterSet uniqueColors() {
        final IntScatterSet tColor = new IntScatterSet();
        for (int i = 0; i < nodeCount; i++) {
            tColor.add(colors.get(i));
        }
        return tColor;
    }

    /**
     * for each distinct color
     * @param consumer color consumer
     */
    public void forEachColor(IntPredicate consumer) {
        final IntScatterSet tColor = new IntScatterSet();
        for (int i = 0; i < nodeCount; i++) {
            final int color = colors.get(i);
            if (tColor.add(color)) {
                if (!consumer.test(color)) {
                    return;
                }
            }
        }
    }

    /**
     * parallel multistep coloring algorithm
     * @param nodeSet
     */
    private void msColorParallel(IntSet nodeSet) {


        final IntStack levelQueue = new IntStack();
        levelQueue.addAll(nodeSet); // TODO

        while (!levelQueue.isEmpty()) {
            System.out.println("nodeSet = " + nodeSet);
            futures.clear();
            if (concurrency <= 1) {
                futures.add(executorService.submit(() -> msColor(levelQueue)));
            } else {
                final int size = levelQueue.size();
                final int batchSize = Math.floorDiv(size, concurrency);
                System.out.println("batchSize = " + batchSize);
                final Iterator<IntCursor> it = levelQueue.iterator();
                for (int i = 0; i < size; i += batchSize) {
                    final IntScatterSet partition = partition(it, batchSize);
                    futures.add(executorService.submit(() -> msColor(partition)));
                }
            }

            final IntSet set = new IntScatterSet();
            // barrier sync (union all future results)
            union(set, futures);
            levelQueue.clear();
            levelQueue.addAll(set);
        }
    }

    /**
     * multistep coloring algorithm
     *
     * @param nodes nodes to process
     * @return container with nodes which must be processed in the next step
     */
    private IntContainer msColor(IntContainer nodes) {

        System.out.println("nodes = " + nodes);

        final IntStack levelQueue = new IntStack(nodes.size());

        nodes.forEach((IntProcedure) node -> {
            final int nodeColor = colors.get(node);
            final boolean[] change = {false};
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                if (cas(targetNodeId, nodeColor)) {
                    if (!visited.get(targetNodeId)) {
                        visited.set(targetNodeId);
                        levelQueue.push(targetNodeId);
                        change[0] = true;
                    }
                }
                return true;
            });
            if (change[0] && !visited.get(node)) {
                levelQueue.push(node);
                visited.set(node);
            }
        });

        return levelQueue;
    }

    /**
     * impl. based on a flippable stack
     *
     * @param nodes
     */
    private void color(IntSet nodes) {

        final FlipStack queue = new FlipStack(nodes.size());
        queue.addAll(nodes);
        queue.flip();

        while (!queue.isEmpty()) {
            queue.forEach(node -> {
                final int nodeColor = colors.get(node);
                final boolean[] change = {false};
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (cas(targetNodeId, nodeColor)) {
                        if (!visited.get(targetNodeId)) {
                            visited.set(targetNodeId);
                            queue.push(targetNodeId);
                            change[0] = true;
                        }
                    }
                    return true;
                });
                if (change[0] && !visited.get(node)) {
                    queue.push(node);
                    visited.set(node);
                }
            });
            queue.clear();
            queue.flip();
        }

    }

    /**
     * simple coloring impl.
     *
     * @param nodes
     */
    private void simpleColor(IntSet nodes) {

        final boolean[] changed = {false};
        do {
            changed[0] = false;
            nodes.forEach((IntProcedure) node -> {
                final int nodeColor = colors.get(node);
                // for each <v, u> in E(V) (direction not specified)
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (cas(targetNodeId, nodeColor)) {
                        changed[0] = true;
                    }
                    return true;
                });
            });

            System.out.println("changed[0] = " + changed[0]);
        } while (changed[0]);
    }

    private void resetColors() {
        for (int i = 0; i < nodeCount; i++) {
            colors.set(i, i);
        }
    }

    /**
     * extracts a node partition
     *
     * @param it the node iterator
     * @param batchSize the maximum batch size
     * @return a batch of nodes with elements <= batchSize
     */
    private IntScatterSet partition(Iterator<IntCursor> it, int batchSize) {
        final IntScatterSet partition = new IntScatterSet(batchSize);
        for (int j = 0; j < batchSize && it.hasNext(); j++) {
            partition.add(it.next().value);
        }
        return partition;
    }

    /**
     * compare and set color only if the new color
     * is greater then the existing
     *
     * @param nodeId the node id
     * @param color the color
     * @return true if color was assigned, false otherwise
     */
    private boolean cas(int nodeId, int color) {
        boolean stored = false;
        while (!stored) {
            int oldC = colors.get(nodeId);
            if (color > oldC) {
                stored = colors.compareAndSet(nodeId, oldC, color);
            } else {
                break;
            }
        }
        return stored;
    }

    /**
     * union all IntContainer from futures into ret set
     * @param ret result set
     * @param futures
     * @return
     */
    private IntSet union(IntSet ret, Collection<Future<IntContainer>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<IntContainer> future : futures) {
                try {
                    future.get().forEach((IntProcedure) ret::add);
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
        return ret;
    }

    // TODO remove
    public void printColor() {
        System.out.println("uniqueColors:");
        for (int i = 0; i < colors.length(); i++) {
            System.out.printf("%d -> %d%n", i, colors.get(i));
        }
    }
}
