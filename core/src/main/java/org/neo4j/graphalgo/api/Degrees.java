package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * returns the degree of a node
 *
 * @author mknblch
 */
public interface Degrees {

    int degree(int nodeId, Direction direction);
}
