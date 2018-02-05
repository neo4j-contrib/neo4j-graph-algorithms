package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    private final Graph graph;
    private final int nodeCount;

    private double m;
    private final double[] ki;
    private final double[] w;

    public Louvain(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        ki = new double[nodeCount];
        w = new double[nodeCount];
    }

    public Louvain compute() {


        return this;
    }

    private void init() {


    }

    private void forEachRelation(RelationshipConsumer consumer) {
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, consumer);
            return true;
        });
    }


    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        return null;
    }
}
