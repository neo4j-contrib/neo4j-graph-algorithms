package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * @author mknblch
 *         added 15.03.2017.
 */
public abstract class GraphFactory {

    protected final GraphDatabaseAPI api;
    protected final String label;
    protected final String relation;
    protected final String property;

    // TODO collections of label/rel/prop ?
    public GraphFactory(
            GraphDatabaseAPI api,
            String label,
            String relation,
            String property) {
        this.api = api;
        this.label = label;
        this.relation = relation;
        this.property = property;
    }

    public abstract Graph build();

    protected final void withReadOps(Consumer<ReadOperations> block) {
        try (final Transaction tx = api.beginTx();
             Statement statement = api
                     .getDependencyResolver()
                     .resolveDependency(ThreadToStatementContextBridge.class)
                     .get()) {
            block.accept(statement.readOperations());
            tx.success();
        }
    }
}
