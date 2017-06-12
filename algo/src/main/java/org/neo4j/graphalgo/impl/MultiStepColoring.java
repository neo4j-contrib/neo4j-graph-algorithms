package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class MultiStepColoring {

    private final Graph graph;
    private final AtomicIntegerArray color;
    private final SimpleBitSet visited;
    private final LinkedBlockingQueue<Integer> queue;

    private final int concurrency = 8;

    public MultiStepColoring(Graph graph, int nodeCount) {
        this.graph = graph;
        color = new AtomicIntegerArray(nodeCount);
        queue = new LinkedBlockingQueue<>();
        for (int i = 0; i < nodeCount; i++) {
            color.set(i, i);
        }
        visited = new SimpleBitSet(nodeCount);

    }

    public IntSet compute(IntSet nodeSet) {

        nodeSet.forEach((IntProcedure) queue::add);

        while (!queue.isEmpty()) {
            final Integer node = queue.poll();
            if (null == node) {
                continue;
            }



        }

    }


    private IntSet color(IntSet nodes) {

        final IntSet tempNodes = new IntScatterSet();

        nodes.forEach((IntProcedure) node -> {

            final int nodeColor = color.get(node);
            boolean[] changed = {false};
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                final int targetColor = color.get(targetNodeId);
                if (nodeColor >= targetColor) {
                    return true;
                }
                color.set(targetNodeId, nodeColor);
                changed[0] |= true;
                if (!visited.contains(targetNodeId)) {
                    visited.put(targetNodeId);
                    queue.add(targetColor);
                }
                return true;
            });
            if (changed[0] && !visited.contains(node)) {
                visited.put(node);
                tempNodes.add(node);
            }
        });

        tempNodes.forEach((IntProcedure) visited::remove);

        return tempNodes;
    }

}
