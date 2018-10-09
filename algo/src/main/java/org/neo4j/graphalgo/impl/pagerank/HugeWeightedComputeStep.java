package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.HugeDegrees;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.api.HugeRelationshipWeights;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class HugeWeightedComputeStep extends HugeBaseComputeStep implements HugeRelationshipConsumer {
    private final HugeRelationshipWeights relationshipWeights;
    private final double[] aggregatedDegrees;
    private double sumOfWeights;
    private double delta;

    HugeWeightedComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
            HugeRelationshipWeights relationshipWeights,
            AllocationTracker tracker,
            int partitionSize,
            long startNode,
            DegreeCache degreeCache) {
        super(dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                tracker,
                partitionSize,
                startNode);
        this.relationshipWeights = relationshipWeights;
        this.aggregatedDegrees = degreeCache.aggregatedDegrees();
    }

    void singleIteration() {
        long startNode = this.startNode;
        long endNode = this.endNode;
        HugeRelationshipIterator rels = this.relationshipIterator;
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            delta = deltas[(int) (nodeId - startNode)];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    sumOfWeights = aggregatedDegrees[(int) nodeId];
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    @Override
    public boolean accept(long sourceNodeId, long targetNodeId) {
        double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);

        if (weight > 0) {
            double proportion = weight / sumOfWeights;
            int srcRankDelta = (int) (100_000 * (delta * proportion));
            if (srcRankDelta != 0) {
                int idx = binaryLookup(targetNodeId, starts);
                nextScores[idx][(int) (targetNodeId - starts[idx])] += srcRankDelta;
            }
        }

        return true;
    }
}
