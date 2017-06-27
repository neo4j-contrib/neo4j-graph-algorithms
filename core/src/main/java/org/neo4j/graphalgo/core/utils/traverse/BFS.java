package org.neo4j.graphalgo.core.utils.traverse;

import org.neo4j.graphdb.Direction;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public interface BFS {

    BFS bfs(int startNodeId, Direction direction, IntPredicate predicate, IntConsumer visitor);

}
