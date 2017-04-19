package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public class GraphUnionFind {

    private final Graph graph;

    private final DisjointSetStruct dss;

    public GraphUnionFind(Graph graph) {
        this.graph = graph;
        this.dss = new DisjointSetStruct(graph.nodeCount());
    }

    public DisjointSetStruct compute() {
        dss.reset();
        graph.forEachNode(node -> {
            graph.forEachOutgoing(node, (source, target, id) -> {
                dss.union(source, target);
                return true;
            });
        });
        return dss;
    }

    public DisjointSetStruct compute(final double threshold) {
        dss.reset();
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, (source, target, id, weight) -> {
                if (weight >= threshold) {
                    dss.union(source, target);
                }
                return true;
            });
        });
        return dss;
    }
}
