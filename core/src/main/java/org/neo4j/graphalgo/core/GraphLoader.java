package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.reflect.InvocationTargetException;

/**
 * @author mknobloch
 */
public class GraphLoader {

    private String label = null;
    private String relation = null;
    private String property = null;

    private final GraphDatabaseAPI api;

    public GraphLoader(GraphDatabaseAPI api) {
        this.api = api;
    }

    public GraphLoader setLabel(String label) {
        this.label = label;
        return this;
    }

    public GraphLoader setRelation(String relation) {
        this.relation = relation;
        return this;
    }

    public GraphLoader setProperty(String property) {
        this.property = property;
        return this;
    }

    public Graph load(Class<? extends GraphFactory> factoryType) {
        try(final Transaction tx = api.beginTx();
            final Statement statement = api
                    .getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()) {
            final ReadOperations readOp = statement.readOperations();
            final Graph graph = factoryType.getConstructor(ReadOperations.class,
                        String.class,
                        String.class,
                        String.class)
                    .newInstance(readOp, label, relation, property)
                    .build();

            tx.success();
            return graph;
        } catch (NoSuchMethodException | IllegalAccessException |
                InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e); // TODO
        }
    }

}