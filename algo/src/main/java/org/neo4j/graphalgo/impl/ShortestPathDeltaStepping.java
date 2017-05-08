package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.RawValues;

public class ShortestPathDeltaStepping {


    private final LongDoubleMap heavy;
    private final LongDoubleMap light;
    private final IntDoubleMap tent;

    private final int nodeCount;
    private final RelationshipWeights weights;
    private final IdMapping idMapping;
    private final AllRelationshipIterator allIterator;

    public ShortestPathDeltaStepping(IdMapping idMapping, AllRelationshipIterator allIterator, RelationshipWeights weights) {
        this.idMapping = idMapping;
        this.allIterator = allIterator;
        this.nodeCount = idMapping.nodeCount();
        this.weights = weights;
        heavy = new LongDoubleScatterMap();
        light = new LongDoubleScatterMap();
        tent = new IntDoubleScatterMap();
    }

    public ShortestPathDeltaStepping compute(long startNode, double delta) {

        split(delta);


        return this;
    }

    private void split(double delta) {
        allIterator.forEachRelationship((sourceNodeId, targetNodeId, relationId) -> {
            double weight = weights.weightOf(sourceNodeId, targetNodeId);
            if (weight > delta) {
                heavy.put(RawValues.combineIntInt(sourceNodeId, targetNodeId), weight);
            } else {
                light.put(RawValues.combineIntInt(sourceNodeId, targetNodeId), weight);
            }
            tent.put(sourceNodeId, Double.MAX_VALUE);
            return true;
        });
    }


}
