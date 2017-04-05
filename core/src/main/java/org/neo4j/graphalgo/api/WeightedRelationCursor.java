package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
public class WeightedRelationCursor extends RelationCursor {

    /**
     * The weight
     */
    public double weight;

    @Override
    public String toString() {
        return "{" + sourceNodeId + ", " + targetNodeId + ", " + weight + "}";
    }
}
