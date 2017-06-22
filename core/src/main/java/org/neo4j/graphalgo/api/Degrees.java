package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * The Degree interface is intended to return the degree
 * of a given node and direction.
 *
 * @author mknblch
 */
public interface Degrees {

    int degree(int nodeId, Direction direction);
}
