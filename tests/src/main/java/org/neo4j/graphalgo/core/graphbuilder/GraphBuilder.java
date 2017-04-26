package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The GraphBuilder intends to ease the creation
 * of test graphs with well known properties
 *
 * @author mknblch
 */
public abstract class GraphBuilder<ME extends GraphBuilder<ME>> {

    protected final HashSet<Node> nodes;
    protected final GraphDatabaseAPI api;
    protected final ThreadToStatementContextBridge bridge;

    protected Transaction tx = null;

    protected String label;
    protected String relationship;

    protected GraphBuilder(GraphDatabaseAPI api, String label, String relationship) {
        this.api = api;
        this.label = label;
        this.relationship = relationship;
        bridge = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        nodes = new HashSet<>();
    }

    public ME setLabel(String label) {
        this.label = label;
        return me();
    }

    public ME setRelationship(String relationship) {
        this.relationship = relationship;
        return me();
    }

    public Relationship createRelationship(Node p, Node q) {
        return p.createRelationshipTo(q, RelationshipType.withName(relationship));
    }

    public Node createNode() {
        Node node = api.createNode();
        if (null != label) {
            node.addLabel(Label.label(label));
        }
        nodes.add(node);
        return node;
    }

    public ME forEachInTx(Consumer<Node> consumer) {
        withinTransaction(() -> nodes.forEach(consumer));
        return me();
    }

    public ME writeInTransaction(Consumer<DataWriteOperations> consumer) {
        beginnTx();
        try(
            Statement statement = bridge.get()) {
            consumer.accept(statement.dataWriteOperations());
        } catch (InvalidTransactionTypeKernelException e) {
            throw new RuntimeException(e);
        }
        closeTx();
        return me();
    }

    public ME withinTransaction(Runnable runnable) {
        beginnTx();
        runnable.run();
        closeTx();
        return me();
    }

    public <T> T withinTransaction(Supplier<T> supplier) {
        beginnTx();
        T t = supplier.get();
        closeTx();
        return t;
    }

    public DefaultBuilder newDefaultBuilder() {
        return new DefaultBuilder(api, label, relationship);
    }

    public RingBuilder newRingBuilder() {
        return new RingBuilder(api, label, relationship);
    }

    protected void beginnTx() {
        if (null != tx) {
            return;
        }
        tx = api.beginTx();
    }

    protected void closeTx() {
        tx.success();
        tx.close();
        tx = null;
    }

    protected abstract ME me();

    public static DefaultBuilder create(GraphDatabaseAPI api) {
        return new DefaultBuilder(api, null, null);
    }
}
