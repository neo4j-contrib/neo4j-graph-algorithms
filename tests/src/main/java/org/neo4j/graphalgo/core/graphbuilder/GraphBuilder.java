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

    /**
     * set the label for all subsequent {@link GraphBuilder#createNode()} operations
     * in the current and in derived builders.
     *
     * @param label the label
     * @return child instance to make methods of the child class accessible.
     */
    public ME setLabel(String label) {
        if (null == label) {
            return self;
        }
        this.label = Label.label(label);
        return self;
    }

    /**
     * set the relationship type for all subsequent {@link GraphBuilder#createRelationship(Node, Node)}
     * operations in the current and in derived builders.
     *
     * @param relationship the name of the relationship type
     * @return child instance to make methods of the child class accessible.
     */
    public ME setRelationship(String relationship) {
        if (null == relationship) {
            return self;
        }
        this.relationship = RelationshipType.withName(relationship);
        return self;
    }

    /**
     * create a relationship between p and q with the previously defined
     * relationship type
     *
     * @param p the source node
     * @param q the target node
     * @return the relationship object
     */
    public Relationship createRelationship(Node p, Node q) {
        return p.createRelationshipTo(q, relationship);
    }

    /**
     * create a new node and set a label if previously defined
     *
     * @return the created node
     */
    public Node createNode() {
        Node node = api.createNode();
        if (null != label) {
            node.addLabel(label);
        }
        nodes.add(node);
        return node;
    }

    /**
     * run node consumer in tx as long as he returns true
     * @param consumer the node consumer
     * @return child instance to make methods of the child class accessible.
     */
    public ME forEachInTx(Consumer<Node> consumer) {
        withinTransaction(() -> nodes.forEach(consumer));
        return self;
    }

    /**
     * runs the write consumer in a transaction
     *
     * @param consumer the write consumer
     * @return child instance to make methods of the child class accessible.
     */
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

    /**
     * run the runnable in a transaction
     *
     * @param runnable the runnable
     * @return child instance to make methods of the child class accessible.
     */
    public ME withinTransaction(Runnable runnable) {
        beginTx();
        runnable.run();
        closeTx();
        return self;
    }

    /**
     * run supplier within a transaction and returns its result
     * @param supplier the supplier
     * @param <T> the return type
     * @return child instance to make methods of the child class accessible.
     */
    public <T> T withinTransaction(Supplier<T> supplier) {
        beginTx();
        T t = supplier.get();
        closeTx();
        return t;
    }

    /**
     * create a new default builder with its own node-set but
     * inherits the current label and relationship type
     *
     * @return a new default builder
     */
    public DefaultBuilder newDefaultBuilder() {
        return new DefaultBuilder(api, label, relationship);
    }

    /**
     * create a new ring builder with its own node-set but
     * inherits current label and relationship type.
     *
     * @return a new ring builder
     */
    public RingBuilder newRingBuilder() {
        return new RingBuilder(api, label, relationship);
    }

    /**
     * create a new transaction if not already open
     */
    protected void beginTx() {
        if (null != tx) {
            return;
        }
        tx = api.beginTx();
    }

    /**
     * close current transaction if any
     */
    protected void closeTx() {
        if (null == tx) {
            return;
        }

        tx.success();
        tx.close();
        tx = null;
    }

    /**
     * return child instance for method chaining from methods of the abstract parent class
     *
     * @return self (child instance)
     */
    protected abstract ME me();

    /**
     * create a new default builder
     *
     * @param api the neo4j api
     * @return a new default builder
     */
    public static DefaultBuilder create(GraphDatabaseAPI api) {
        return new DefaultBuilder(api, null, null);
    }
}
