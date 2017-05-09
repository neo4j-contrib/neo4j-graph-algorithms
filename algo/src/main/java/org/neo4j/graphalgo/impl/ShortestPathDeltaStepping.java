package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.RawValues;

public class ShortestPathDeltaStepping {

    private final IntDoubleMap tent;

    private final int nodeCount;

    private final RelationshipWeights weights;
    private final double delta;
    private final IdMapping idMapping;
    private final AllRelationshipIterator allIterator;

    public ShortestPathDeltaStepping(IdMapping idMapping, AllRelationshipIterator allIterator, RelationshipWeights weights, double delta) {
        this.idMapping = idMapping;
        this.allIterator = allIterator;
        this.nodeCount = idMapping.nodeCount();
        this.weights = weights;
        this.delta = delta;
        tent = new IntDoubleScatterMap();
    }

    public ShortestPathDeltaStepping compute(long startNode, double delta) {



        return this;
    }

    private void relax(int nodeId, double cost) {

    }

}
