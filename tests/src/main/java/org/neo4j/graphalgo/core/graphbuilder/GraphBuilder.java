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

    private final ME self;

    protected final HashSet<Node> nodes;
    protected final GraphDatabaseAPI api;
    protected final ThreadToStatementContextBridge bridge;

    protected Transaction tx = null;

    protected Label label;
    protected RelationshipType relationship;

    protected GraphBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        this.api = api;
        this.label = label;
        this.relationship = relationship;
        bridge = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        nodes = new HashSet<>();
        this.self = me();
    }

    public ME setLabel(String label) {
        if (null == label) {
            return self;
        }
        this.label = Label.label(label);
        return self;
    }

    public ME setRelationship(String relationship) {
        if (null == relationship) {
            return self;
        }
        this.relationship = RelationshipType.withName(relationship);
        return self;
    }

    public Relationship createRelationship(Node p, Node q) {
        return p.createRelationshipTo(q, relationship);
    }

    public Node createNode() {
        Node node = api.createNode();
        if (null != label) {
            node.addLabel(label);
        }
        nodes.add(node);
        return node;
    }

    public ME forEachInTx(Consumer<Node> consumer) {
        withinTransaction(() -> nodes.forEach(consumer));
        return self;
    }

    public ME writeInTransaction(Consumer<DataWriteOperations> consumer) {
        beginTx();
        try(
            Statement statement = bridge.get()) {
            consumer.accept(statement.dataWriteOperations());
        } catch (InvalidTransactionTypeKernelException e) {
            throw new RuntimeException(e);
        }
        closeTx();
        return self;
    }

    public ME withinTransaction(Runnable runnable) {
        beginTx();
        runnable.run();
        closeTx();
        return self;
    }

    public <T> T withinTransaction(Supplier<T> supplier) {
        beginTx();
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

    protected void beginTx() {
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
