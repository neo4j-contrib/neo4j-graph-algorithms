package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

public class NonWeightedPageRankVariant implements PageRankVariant {
    public ComputeStep createComputeStep(double dampingFactor, int[] sourceNodeIds,
                                         RelationshipIterator relationshipIterator,
                                         WeightedRelationshipIterator weightedRelationshipIterator,
                                         Degrees degrees,
                                         int partitionCount, int start,
                                         DegreeCache degreeCache) {
        return new NonWeightedComputeStep(
                dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                partitionCount,
                start
        );
    }

    @Override
    public HugeNonWeightedComputeStep createHugeComputeStep(double dampingFactor, long[] sourceNodeIds, HugeRelationshipIterator relationshipIterator, HugeDegrees degrees, HugeRelationshipWeights relationshipWeights, AllocationTracker tracker, int partitionCount, long start, DegreeCache aggregatedDegrees) {
        return new HugeNonWeightedComputeStep(
                dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                tracker,
                partitionCount,
                start
        );
    }

    @Override
    public DegreeComputer degreeComputer(Graph graph) {
        return new NoOpDegreeComputer();
    }
}
