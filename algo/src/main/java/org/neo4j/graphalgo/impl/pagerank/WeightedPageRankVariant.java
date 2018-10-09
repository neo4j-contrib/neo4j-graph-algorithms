package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

public class WeightedPageRankVariant implements PageRankVariant {
    private boolean cacheWeights;

    public WeightedPageRankVariant(boolean cacheWeights) {
        this.cacheWeights = cacheWeights;
    }


    public ComputeStep createComputeStep(double dampingFactor, int[] sourceNodeIds,
                                         RelationshipIterator relationshipIterator,
                                         WeightedRelationshipIterator weightedRelationshipIterator,
                                         Degrees degrees,
                                         int partitionCount, int start,
                                         DegreeCache degreeCache) {
        if(cacheWeights ){
            return new WeightedWithCachedWeightsComputeStep(
                    dampingFactor,
                    sourceNodeIds,
                    relationshipIterator,
                    degrees,
                    partitionCount,
                    start,
                    degreeCache
            );
        } else {
            return new WeightedComputeStep(
                    dampingFactor,
                    sourceNodeIds,
                    weightedRelationshipIterator,
                    degrees,
                    partitionCount,
                    start,
                    degreeCache
            );
        }
    }

    @Override
    public HugeComputeStep createHugeComputeStep(double dampingFactor, long[] sourceNodeIds,
                                                 HugeRelationshipIterator relationshipIterator, HugeDegrees degrees,
                                                 HugeRelationshipWeights relationshipWeights, AllocationTracker tracker,
                                                 int partitionCount, long start, DegreeCache aggregatedDegrees) {
        return new HugeWeightedComputeStep(
                dampingFactor,
                sourceNodeIds,
                relationshipIterator,
                degrees,
                relationshipWeights,
                tracker,
                partitionCount,
                start,
                aggregatedDegrees
        );
    }

    @Override
    public DegreeComputer degreeComputer(Graph graph) {
            return new WeightedDegreeComputer(graph, cacheWeights);
    }
}
