package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.DoubleDoubleHashMap;
import com.carrotsearch.hppc.DoubleDoubleMap;
import com.carrotsearch.hppc.IntDoubleHashMap;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.cursors.DoubleDoubleCursor;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.Direction;

import java.util.function.IntConsumer;

public final class LabelPropagation implements IntConsumer, RelationshipConsumer {

    private final HeavyGraph graph;
    private Direction direction;
    private DoubleDoubleMap votes;
    private IntDoubleMap labels;


    public LabelPropagation(final HeavyGraph graph) {
        this.graph = graph;
    }

    public IntDoubleMap compute(Direction direction, long times) {
        this.direction = direction;
        votes = new DoubleDoubleHashMap();
        labels = new IntDoubleHashMap(graph.nodeCount());

        for (long i = 0; i < times; i++) {
            graph.forEachNode(this);
        }

        return labels;
    }


    @Override
    public void accept(final int node) {
        votes.clear();
        graph.forEachRelationship(node, direction, this);

        double originalPartition = partition(node);
        double partition = originalPartition;
        double weight = Double.NEGATIVE_INFINITY;
        for (DoubleDoubleCursor vote : votes) {
            if (weight < vote.value) {
                weight = vote.value;
                partition = vote.key;
            }
        }

        if (partition != originalPartition) {
            labels.put(node, partition);
        }
    }

    @Override
    public boolean accept(
            final int sourceNodeId,
            final int targetNodeId,
            final long relationId) {
        double partition = partition(targetNodeId);
        double weight = graph.weightOf(sourceNodeId, targetNodeId) * graph.weightOf(targetNodeId);
        votes.addTo(partition, weight);
        return true;
    }

    private double partition(int node) {
        double partition = labels.getOrDefault(node, Double.NEGATIVE_INFINITY);
        return partition == Double.NEGATIVE_INFINITY ? graph.valueOf(node, node) : partition;
    }
}
