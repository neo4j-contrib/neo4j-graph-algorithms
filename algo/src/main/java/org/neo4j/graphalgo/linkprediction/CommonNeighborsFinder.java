package org.neo4j.graphalgo.linkprediction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.neo4j.graphdb.Direction.*;

public class CommonNeighborsFinder {

    private GraphDatabaseAPI api;

    public CommonNeighborsFinder(GraphDatabaseAPI api) {
        this.api = api;
    }

    public Set<Node> findCommonNeighbors(Node node1, Node node2, RelationshipType relationshipType, Direction direction) {
        if(node1.equals(node2)) {
            return Collections.emptySet();
        }

        Set<Node> neighbors = findPotentialNeighbors(node1, relationshipType, direction);
        neighbors.removeIf(node -> noCommonNeighbors(node, relationshipType, flipDirection(direction), node2));
        return neighbors;
    }

    private Direction flipDirection(Direction direction) {
        switch(direction) {
            case OUTGOING:
                return INCOMING;
            case INCOMING:
                return OUTGOING;
            default:
                return BOTH;
        }
    }

    private Set<Node> findPotentialNeighbors(Node node, RelationshipType relationshipType, Direction direction) {
        Set<Node> neighbors = new HashSet<>();

        for (Relationship rel : loadRelationships(node, relationshipType, direction)) {
            Node endNode = rel.getOtherNode(node);

            if (!endNode.equals(node)) {
                neighbors.add(endNode);
            }
        }
        return neighbors;
    }

    private boolean noCommonNeighbors(Node node, RelationshipType relationshipType, Direction direction, Node node2) {
        for (Relationship rel : loadRelationships(node, relationshipType, direction)) {
            if (rel.getOtherNode(node).equals(node2)) {
                return false;
            }
        }
        return true;
    }

    private Iterable<Relationship> loadRelationships(Node node, RelationshipType relationshipType, Direction direction) {
        return relationshipType == null ? node.getRelationships(direction) : node.getRelationships(relationshipType, direction);
    }

}
