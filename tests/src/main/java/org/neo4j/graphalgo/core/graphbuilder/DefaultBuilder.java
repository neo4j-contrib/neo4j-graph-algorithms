package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class DefaultBuilder extends GraphBuilder<DefaultBuilder> {

    protected DefaultBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    @Override
    public Relationship createRelationship(Node p, Node q) {
        beginTx();
        Relationship relationship = super.createRelationship(p, q);
        closeTx();
        return relationship;
    }

    @Override
    public Node createNode() {
        beginTx();
        Node node = super.createNode();
        closeTx();
        return node;
    }

    @Override
    protected DefaultBuilder me() {
        return this;
    }
}
