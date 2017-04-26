package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.core.utils.RawValues;

/**
 * @author mknobloch
 */
@Deprecated
public interface WeightMapping {

    /**
     * returns the weight for ID if set or the load-time specified default weight otherwise
     */
    double get(long id);

    /**
     * returns the weight for ID if set or the given default weight otherwise
     */
    double get(long id, double defaultValue);

    default double get(int source, int target) {
        return get(RawValues.combineIntInt(source, target));
    }

    default double get(int id) {
        return get(RawValues.combineIntInt(id, -1));
    }

    default double get(int id, double defaultValue) {
        return get(RawValues.combineIntInt(id, -1));
    }

    /**
     * set the weight for ID
     */
    void set(long id, Object weight); // TODO rm?

    default void set(int id, Object weight) {
        set(RawValues.combineIntInt(id, -1), weight);
    }
}
