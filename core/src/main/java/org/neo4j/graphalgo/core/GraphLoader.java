package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;

/**
 * @author mknobloch
 */
public class GraphLoader {

    private String label = null;
    private String relation = null;
    private String property = null;
    private ExecutorService executorService = null;

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

    public GraphLoader setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public GraphLoader setProperty(String property) {
        this.property = property;
        return this;
    }

    public Graph load(Class<? extends GraphFactory> factoryType) {
        try {
            return factoryType.getConstructor(
                    GraphDatabaseAPI.class,
                    String.class,
                    String.class,
                    String.class,
                    ExecutorService.class)
                    .newInstance(api, label, relation, property, executorService)
                    .build();
        } catch (NoSuchMethodException | IllegalAccessException |
                InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e); // TODO
        }
    }

}
