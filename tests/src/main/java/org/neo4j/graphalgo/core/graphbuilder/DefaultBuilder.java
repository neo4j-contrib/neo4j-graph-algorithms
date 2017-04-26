package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class DefaultBuilder extends GraphBuilder<DefaultBuilder> {

    protected DefaultBuilder(GraphDatabaseAPI api, String label, String relationship) {
        super(api, null, null);
    }

    @Override
    public Relationship createRelationship(Node p, Node q) {
        beginnTx();
        Relationship relationship = super.createRelationship(p, q);
        closeTx();
        return relationship;
    }

    @Override
    public Node createNode() {
        beginnTx();
        Node node = super.createNode();
        closeTx();
        return node;
    }

    @Override
    protected DefaultBuilder me() {
        return this;
    }
}
