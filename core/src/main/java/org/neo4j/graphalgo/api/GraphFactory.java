package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * The Abstract Factory defines the construction of the graph
 *
 * @author mknblch
 */
public abstract class GraphFactory {

    private ThreadToStatementContextBridge contextBridge;

    protected final GraphDatabaseAPI api;
    protected final GraphSetup setup;

    public GraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    public abstract Graph build();

    /**
     * executes a consumer within its own transaction
     *
     * @param block the consumer
     */
    protected final void withReadOps(Consumer<ReadOperations> block) {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            block.accept(statement.readOperations());
            tx.success();
        }
    }
}
