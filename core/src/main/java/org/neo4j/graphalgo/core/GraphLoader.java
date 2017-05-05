package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * The GraphLoader provides a fluent interface and default values to configure
 * the {@link Graph} before loading it.
 * <p>
 * By default, the complete graph is loaded – no restriction based on
 * node label or relationship type is made.
 * Weights are also not loaded by default.
 *
 * @author mknobloch
 */
public class GraphLoader {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final MethodType CTOR_METHOD = MethodType.methodType(
            void.class,
            GraphDatabaseAPI.class,
            GraphSetup.class);

    private String label = null;
    private String relation = null;
    private String relWeightProp = null;
    private String nodeWeightProp = null;
    private String nodeProp = null;

    private final GraphDatabaseAPI api;
    private ExecutorService executorService;
    private double relWeightDefault = 0.0;
    private double nodeWeightDefault = 0.0;
    private double nodePropDefault = 0.0;
    /** statement to load nodes, has to return "id" and optionally "weight" or "value" */
    private String nodeStatement;
    /** statement to load unique relationships, has to return ids of start "source" and end-node "target" and optionally "weight" */
    private String relationshipStatement;

    /**
     * Creates a new serial GraphLoader.
     */
    public GraphLoader(GraphDatabaseAPI api) {
        this.api = Objects.requireNonNull(api);
        this.executorService = null;
    }

    /**
     * Creates a new parallel GraphLoader.
     * What exactly parallel means depends on the {@link GraphFactory}
     * implementation provided in {@link #load(Class)}.
     */
    public GraphLoader(GraphDatabaseAPI api, ExecutorService executorService) {
        this.api = Objects.requireNonNull(api);
        this.executorService = Objects.requireNonNull(executorService);
    }

    /**
     * set an executor service
     * @param executorService the executor service
     * @return itself to enable fluent interface
     */
    public GraphLoader withExecutorService(ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService);
        return this;
    }

    /**
     * disable use of executor service
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutExecutorService() {
        this.executorService = null;
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     * If the label is not found, every node will be loaded.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withLabel(String label) {
        this.label = Objects.requireNonNull(label);
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     * If the label is not found, every node will be loaded. TODO review that
     *
     * @param label May be null
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalLabel(String label) {
        this.label = label;
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given {@link Label}.
     * If the label is not found, every node will be loaded.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withLabel(Label label) {
        this.label = Objects.requireNonNull(label).name();
        return this;
    }

    /**
     * Instructs the loader to load any node with no restriction to any label.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withAnyLabel() {
        this.label = null;
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     * If the type is not found, every relationship will be loaded. TODO review that
     *
     * @param relation May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipType(String relation) {
        this.relation = Objects.requireNonNull(relation);
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     * If the argument is null, every relationship will be considered.
     *
     * @param relation May be null
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalRelationshipType(String relation) {
        this.relation = relation;
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given {@link RelationshipType}.
     * If the type is not found, every relationship will be loaded.
     *
     * @param relation May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipType(RelationshipType relation) {
        this.relation = Objects.requireNonNull(relation).name();
        return this;
    }

    /**
     * Instructs the loader to load any relationship with no restriction to any type.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withAnyRelationshipType() {
        this.relation = null;
        return this;
    }

    /**
     * Instructs the loader to load relationship weights by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property May not be null; to remove a weight property, use {@link #withoutRelationshipWeights()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipWeightsFromProperty(String property, double propertyDefaultValue) {
        this.relWeightProp = Objects.requireNonNull(property);
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load relationship weights by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalRelationshipWeightsFromProperty(String property, double propertyDefaultValue) {
        this.relWeightProp = property;
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node weights by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property May not be null; to remove a weight property, use {@link #withoutNodeWeights()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withNodeWeightsFromProperty(String property, double propertyDefaultValue) {
        this.nodeWeightProp = Objects.requireNonNull(property);
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node weights by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalNodeWeightsFromProperty(String property, double propertyDefaultValue) {
        this.nodeWeightProp = property;
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node values by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property May not be null; to remove a node property, use {@link #withoutNodeProperties()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withNodeProperty(String property, double propertyDefaultValue) {
        this.nodeProp = Objects.requireNonNull(property);
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node values by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalNodeProperty(String property, double propertyDefaultValue) {
        this.nodeProp = property;
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any relationship weights. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultRelationshipWeight(double propertyDefaultValue) {
        this.relWeightProp = null;
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any node weights. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultNodeWeight(double propertyDefaultValue) {
        this.nodeWeightProp = null;
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any node properties. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultNodeProperties(double propertyDefaultValue) {
        this.nodeProp = null;
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any weights. The behavior of using weighted graph-functions
     * on a graph without weights is not specified.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutRelationshipWeights() {
        this.relWeightProp = null;
        this.relWeightDefault = 0.0;
        return this;
    }

    /**
     * Instructs the loader to not load any node weights.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutNodeWeights() {
        this.nodeWeightProp = null;
        this.nodeWeightDefault = 0.0;
        return this;
    }

    /**
     * Instructs the loader to not load any node properties.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutNodeProperties() {
        this.nodeProp = null;
        this.nodePropDefault = 0.0;
        return this;
    }

    /**
     * Loads the graph using the provided GraphFactory, passing the built
     * configuration as parameters.
     * <p>
     * The chosen implementation determines the performance characteristics
     * during load and usage of the Graph.
     *
     * @return the freshly loaded graph
     */
    public Graph load(Class<? extends GraphFactory> factoryType) {
        final MethodHandle constructor = findConstructor(factoryType);
        return invokeConstructor(constructor).build();
    }

    private MethodHandle findConstructor(Class<?> factoryType) {
        try {
            return LOOKUP.findConstructor(factoryType, CTOR_METHOD);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphFactory invokeConstructor(MethodHandle constructor) {

        final GraphSetup setup = new GraphSetup(
                label,
                null,
                relation,
                relWeightProp,
                relWeightDefault,
                nodeWeightProp,
                nodeWeightDefault,
                nodeProp,
                nodePropDefault,
                executorService,
                nodeStatement,
                relationshipStatement);

        try {
            return (GraphFactory) constructor.invoke(api, setup);
        } catch (Throwable throwable) {
            throw Exceptions.launderedException(
                    throwable.getMessage(),
                    throwable);
        }
    }

    /**
     * provide statement to load nodes, has to return "id" and optionally "weight" or "value"
     * @param nodeStatement
     * @return
     */
    public GraphLoader withNodeStatement(String nodeStatement) {
        this.nodeStatement = nodeStatement;
        return this;
    }

    /**
     * provide statement to load unique relationships, has to return ids of start "source" and end-node "target" and optionally "weight"
     * @param relationshipStatement
     * @return
     */
    public GraphLoader withRelationshipStatement(String relationshipStatement) {
        this.relationshipStatement = relationshipStatement;
        return this;
    }
}
