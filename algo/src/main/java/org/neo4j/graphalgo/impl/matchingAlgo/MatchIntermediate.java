package org.neo4j.graphalgo.impl.matchingAlgo;

import org.neo4j.graphdb.Node;

public class MatchIntermediate {
    private Node node;
    private Node originNode;
    private Node destNode;
    private int firstLegDistance;
    private int lastLegDistance;

    public MatchIntermediate() {
    }

    public int getFirstLegDistance() {
        return firstLegDistance;
    }

    public void setFirstLegDistance(int firstLegDistance) {
        this.firstLegDistance = firstLegDistance;
    }

    public int getLastLegDistance() {
        return lastLegDistance;
    }

    public void setLastLegDistance(int lastLegDistance) {
        this.lastLegDistance = lastLegDistance;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Node getOriginNode() {
        return originNode;
    }

    public void setOriginNode(Node originNode) {
        this.originNode = originNode;
    }

    public Node getDestNode() {
        return destNode;
    }

    public void setDestNode(Node destNode) {
        this.destNode = destNode;
    }
}
