package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class HugeArticleRankComputeStep extends HugeBaseComputeStep implements HugeRelationshipConsumer {
    private final HugeRelationshipIterator relationshipIterator;
    private double averageDegree;

    HugeArticleRankComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
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
        this.relationshipIterator = relationshipIterator;
        this.averageDegree = degreeCache.average();
    }


    private int srcRankDelta;


    void singleIteration() {
        long startNode = this.startNode;
        long endNode = this.endNode;
        HugeRelationshipIterator rels = this.relationshipIterator;
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            double delta = deltas[(int) (nodeId - startNode)];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    srcRankDelta = (int) (100_000 * (delta / (degree + averageDegree)));
                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    public boolean accept(long sourceNodeId, long targetNodeId) {
        if (srcRankDelta != 0) {
            int idx = binaryLookup(targetNodeId, starts);
            nextScores[idx][(int) (targetNodeId - starts[idx])] += srcRankDelta;
        }
        return true;
    }
}
