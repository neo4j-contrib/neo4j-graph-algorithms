package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * UF implementation using a {@link NodeIterator} and {@link WeightedRelationshipIterator}
 *
 * @author mknblch
 */
public class GraphUnionFind {

    private final Graph graph;

    private final DisjointSetStruct dss;

    public GraphUnionFind(Graph graph) {
        this.graph = graph;
        this.dss = new DisjointSetStruct(graph.nodeCount());
    }

    /**
     * compute unions of connected nodes
     * @return a DSS
     */
    public DisjointSetStruct compute() {
        dss.reset();
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target, id) -> {
                dss.union(source, target);
                return true;
            });
            return true;
        });
        return dss;
    }

    /**
     * compute unions using an additional constraint
     * @param threshold the minimum threshold
     * @return a DSS
     */
    public DisjointSetStruct compute(final double threshold) {
        dss.reset();
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target, id, weight) -> {
                if (weight >= threshold) {
                    dss.union(source, target);
                }
                return true;
            });
            return true;
        });
        return dss;
    }
}
