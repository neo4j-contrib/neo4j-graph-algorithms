/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * Intersect adjacency lists of two nodes.
 * Only {@link org.neo4j.graphdb.Direction#OUTGOING} is used for intersection.
 * If you want to intersect on {@link org.neo4j.graphdb.Direction#BOTH}, you have to
 * load the graph with {@link org.neo4j.graphalgo.core.GraphLoader#asUndirected(boolean)}
 * set to {@code true}.
 *
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link HugeRelationshipIterator}s.
 */
public interface HugeRelationshipIntersect {

    /**
     * @see HugeDegrees#degree(long, Direction)
     */
    int degree(long nodeId);

    /**
     * @see HugeRelationshipIterator#forEachOutgoing(long, HugeRelationshipConsumer)
     */
    void forEachRelationship(long nodeId, HugeRelationshipConsumer consumer);

    /**
     * Intersect the adjacency lists of {@code nodeIdA} and {@code nodeIdB} and write
     * the found ids into {@code result}, starting at {@code resultOffset}.
     * It is the responsibility of the caller to ensure that {@code result} is long enough,
     * usually by using degree such as
     * <pre>
     *     int length = Math.min(degree(nodeIdA), degree(nodeIdB));
     *     long[] result = new long[length];
     *     length = intersect(nodeIdA, nodeIdB, result, 0);
     *     result = Arrays.copyOf(result, length)
     * </pre>
     * @return the number of target ids written to the result array.
     */
    int intersect(long nodeIdA, long nodeIdB, long[] result, int resultOffset);

    /**
     * Intersect the adjacency lists of {@code nodeIdA} and {@code nodeIdB}.
     * For performance sensitive call-sites, {@link #intersect(long, long, long[], int)} is
     * generally preferred as the result array can be reused by the caller.
     * @return an array of result ids
     */
    default long[] intersect(long nodeIdA, long nodeIdB) {
        int length = Math.min(degree(nodeIdA), degree(nodeIdB));
        long[] result = new long[length];
        length = intersect(nodeIdA, nodeIdB, result, 0);
        return length < result.length ? Arrays.copyOf(result, length) : result;
    }
}
