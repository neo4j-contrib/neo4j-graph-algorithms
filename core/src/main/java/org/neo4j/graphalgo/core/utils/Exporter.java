package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * @author mknblch
 */
public abstract class Exporter<T> {

    protected final GraphDatabaseAPI api;
    protected final ThreadToStatementContextBridge bridge;

    public Exporter(GraphDatabaseAPI api) {
        this.api = api;
        this.bridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    public abstract void write(T data);

    protected int getOrCreatePropertyId(String propertyName) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            int propertyId = statement
                    .tokenWriteOperations()
                    .propertyKeyGetOrCreateForName(propertyName);
            tx.success();
            return propertyId;
        } catch (IllegalTokenNameException e) {
            throw new RuntimeException(e);
        }
    }

    protected void readInTransaction(Consumer<ReadOperations> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            consumer.accept(statement.readOperations());
            tx.success();
        }
    }

    protected void writeInTransaction(Consumer<DataWriteOperations> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            consumer.accept(statement.dataWriteOperations());
            tx.success();
        } catch (InvalidTransactionTypeKernelException e) {
            throw new RuntimeException(e);
        }
    }
}
