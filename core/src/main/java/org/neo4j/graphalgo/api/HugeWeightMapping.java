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

    /**
     * release internal data structures and return an estimate how many
     * bytes were freed.
     * The mapping is not usable afterwards.
     */
    long release();
}
