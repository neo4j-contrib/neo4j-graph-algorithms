package org.neo4j.graphalgo.api;

public interface HugeWeightMapping {

    /**
     * returns the weight for the relationship defined by their start and end nodes
     */
    double weight(long source, long target);

    /**
     * returns the weight for the relationship defined by their start and end nodes
     * or the default value if no such weight exists
     */
    double weight(long source, long target, double defaultValue);
}
