package org.neo4j.graphalgo.core.heavyweight.mst;

public class Edge {

    public final int sourceNode;
    public final int targetNode;
    public final double weight;

    public Edge(int sourceNode, int targetNode, double weight) {
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "sourceNode=" + sourceNode +
                ", targetNode=" + targetNode +
                ", weight=" + weight +
                '}';
    }
}