package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
public interface WeightMapping {

    /**
     * returns the weight for ID if set or the default weight otherwise
     */
    double get(long id);

    /**
     * set the weight for ID
     */
    void set(long id, Object weight); // TODO rm?
}
