package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class NonWeightedComputeStep extends BaseComputeStep implements RelationshipConsumer  {

    private final RelationshipIterator relationshipIterator;

    NonWeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode) {
        super(dampingFactor, sourceNodeIds, degrees, partitionSize, startNode);
        this.relationshipIterator = relationshipIterator;
    }


    private int srcRankDelta;


    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        RelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    srcRankDelta = (int) (100_000 * (delta / degree));
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        if (srcRankDelta != 0) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
        }
        return true;
    }
}
