package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class WeightedComputeStep extends BaseComputeStep implements RelationshipConsumer {
    private final RelationshipWeights relationshipWeights;
    private final double[] aggregatedDegrees;
    private double sumOfWeights;
    private double delta;

    WeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            RelationshipWeights relationshipWeights,
            int partitionSize,
            int startNode, double[] aggregatedDegrees) {
        super(dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                partitionSize,
                startNode);
        this.relationshipWeights = relationshipWeights;
        this.aggregatedDegrees = aggregatedDegrees;
    }

    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        RelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    sumOfWeights = aggregatedDegrees[nodeId];

                    rels.forEachRelationship(nodeId, Direction.OUTGOING,this);
                }
            }
        }
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);

        if(weight > 0) {
            double proportion = weight / sumOfWeights;
            int srcRankDelta = (int) (100_000 * (delta * proportion));
            if (srcRankDelta != 0) {
                int idx = binaryLookup(targetNodeId, starts);
                nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
            }
        }

        return true;
    }
}
