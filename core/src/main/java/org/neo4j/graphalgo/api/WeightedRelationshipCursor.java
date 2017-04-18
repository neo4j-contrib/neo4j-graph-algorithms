package org.neo4j.graphalgo.api;

/**
 * @author mknobloch
 */
@Deprecated
public class WeightedRelationshipCursor extends RelationshipCursor {

    /**
     * The weight
     */
    public double weight;

    @Override
    public String toString() {
        return "{" + sourceNodeId + ", " + targetNodeId + ", " + weight + "}";
    }
}
