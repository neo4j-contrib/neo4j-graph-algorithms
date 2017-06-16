package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class MultiStepTarjan implements RelationshipConsumer {

    private final Graph graph;

    private final ObjectArrayList<IntSet> connectedComponents;

    private BitSet onStack;
    private IntIntMap indices;
    private IntIntMap lowLink;
    private IntStack stack;

    private int index;

    public MultiStepTarjan(Graph graph, ObjectArrayList<IntSet> connectedComponents) {
        this.graph = graph;
        this.connectedComponents = connectedComponents;
    }

    public ObjectArrayList<IntSet> compute(IntSet nodes) {

        indices = new IntIntScatterMap(nodes.size());
        lowLink = new IntIntScatterMap(nodes.size());
        stack = new IntStack(nodes.size());
        onStack = new BitSet();
        nodes.forEach((IntProcedure) this::strongConnect);
        return connectedComponents;
    }

    private void reset() {
        connectedComponents.clear();
        indices.clear();
        lowLink.clear();
        onStack.clear();
        stack.clear();
        index = 0;
    }

    private void strongConnect(int node) {
        if (indices.containsKey(node)) {
            return;
        }
        lowLink.put(node, index);
        indices.put(node, index);
        index++;
        stack.push(node);
        onStack.set(node);
        graph.forEachRelationship(node, Direction.OUTGOING, this);
        if (indices.get(node) == lowLink.get(node)) {
            relax(node);
        }
    }

    private void relax(int nodeId) {
        IntHashSet connected = new IntHashSet();
        int w;
        do {
            w = stack.pop();
            onStack.clear(w);
            connected.add(w);
        } while (w != nodeId);
        connectedComponents.add(connected);
    }

    @Override
    public boolean accept(int source, int target, long edgeId) {
        if (!indices.containsKey(target)) {
            strongConnect(target);
            lowLink.put(source, Math.min(lowLink.get(source), lowLink.get(target)));
        } else if (onStack.get(target)) {
            lowLink.put(source, Math.min(lowLink.get(source), indices.get(target)));
        }
        return true;
    }
}
