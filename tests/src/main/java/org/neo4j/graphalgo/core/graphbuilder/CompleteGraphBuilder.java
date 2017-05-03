package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;

/**
 * Builds a complete graph where all nodes are interconnected
 *
 * @author mknblch
 */
public class CompleteGraphBuilder extends GraphBuilder<CompleteGraphBuilder> {

    protected CompleteGraphBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    public CompleteGraphBuilder createCompleteGraph(int nodeCount) {
        ArrayList<Node> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(createNode());
        }
        for (int i = 0; i < nodeCount; i++) {
            for (int j = 0; j < nodeCount; j++) {
                if (i == j) {
                    continue;
                }
                createRelationship(nodes.get(i), nodes.get(j));
            }
        }
        return this;
    }

    @Override
    protected CompleteGraphBuilder me() {
        return this;
    }
}
