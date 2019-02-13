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
package org.neo4j.graphalgo.linkprediction;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class LinkPrediction {
    @Context
    public GraphDatabaseAPI api;

    @UserFunction("algo.linkprediction.adamicAdar")
    @Description("algo.linkprediction.adamicAdar(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Adamic Adar similarity")
    public double adamicAdarSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://en.wikipedia.org/wiki/Adamic/Adar_index

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new CommonNeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / Math.log(degree(relationshipType, direction, nb))).sum();
    }

    @UserFunction("algo.linkprediction.resourceAllocation")
    @Description("algo.linkprediction.resourceAllocation(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Resource Allocation similarity")
    public double resourceAllocationSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://arxiv.org/pdf/0901.0553.pdf

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new CommonNeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / degree(relationshipType, direction, nb)).sum();
    }

    @UserFunction("algo.linkprediction.commonNeighbors")
    @Description("algo.linkprediction.commonNeighbors(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, returns the number of common neighbors")
    public double commonNeighbors(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new CommonNeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.size();
    }


    private int degree(RelationshipType relationshipType, Direction direction, Node node) {
        return relationshipType == null ? node.getDegree(direction) : node.getDegree(relationshipType, direction);
    }

}
