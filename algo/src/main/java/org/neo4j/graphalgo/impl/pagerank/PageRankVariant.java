package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;


public interface PageRankVariant {
    ComputeStep createComputeStep(double dampingFactor, int[] sourceNodeIds,
                                  RelationshipIterator relationshipIterator,
                                  WeightedRelationshipIterator weightedRelationshipIterator, Degrees degrees,
                                  int partitionCount, int start,
                                  DegreeCache degreeCache);

    HugeComputeStep createHugeComputeStep(double dampingFactor, long[] sourceNodeIds,
                                          HugeRelationshipIterator relationshipIterator, HugeDegrees degrees,
                                          HugeRelationshipWeights relationshipWeights, AllocationTracker tracker,
                                          int partitionCount, long start, DegreeCache aggregatedDegrees);

    DegreeComputer degreeComputer(Graph graph);

}


