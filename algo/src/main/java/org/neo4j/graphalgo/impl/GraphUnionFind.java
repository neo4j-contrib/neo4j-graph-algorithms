package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

/**
 * Sequential UnionFind:
 *
 * The algorithm computes sets of weakly connected components.
 *
 * The impl. is based on the {@link DisjointSetStruct}. It iterates over all relationships once
 * within a single forEach loop and adds each source-target pair to the struct. Therefore buffering
 * would introduce an overhead (a SingleRun-RelationshipIterable might be used here).
 *
 * There are 2 different methods for computing the component-sets. compute() calculates all weakly
 * components regardless of the actual weight of the relationship while compute(threshold:double)
 * on the other hand only takes the transition into account if the weight exceeds the threshold value.
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
     * compute unions if relationship weight exceeds threshold
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
