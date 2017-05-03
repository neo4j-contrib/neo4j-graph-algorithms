package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * The class intends to ease the import of data sources
 *
 * @author mknblch
 */
public abstract class Importer<T, ME extends Importer<T, ME>> {

    public static final double DEFAULT_WEIGHT = 0.0;

    /**
     * neo4j core api
     */
    protected final GraphDatabaseAPI api;
    protected final ThreadToStatementContextBridge bridge;

    /**
     * basic properties
     */
    protected String label = null;
    protected String endLabel = null; // TODO

    protected String relationship = null;

    protected String property = null;
    protected double propertyDefaultValue = DEFAULT_WEIGHT;

    /**
     * resolved values for labelId, relationType and propertyType
     */
    protected int labelId = ReadOperations.ANY_LABEL;
    protected int endLabelId = ReadOperations.ANY_LABEL;
    protected int[] relationId = null;
    protected int propertyId = StatementConstants.NO_SUCH_PROPERTY_KEY;

    /**
     * number of nodes
     */
    protected int nodeCount = 0;

    public Importer(GraphDatabaseAPI api) {
        this.api = api;
        this.bridge = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    public T build() {

        withinTransaction(readOp -> {
            labelId = loadAnyLabel()
                    ? ReadOperations.ANY_LABEL
                    : readOp.labelGetForName(label);
            endLabelId = endLabel == null
                    ? ReadOperations.ANY_LABEL
                    : readOp.labelGetForName(label);
            if (!loadAnyRelationship()) {
                int relId = readOp.relationshipTypeGetForName(relationship);
                if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                    relationId = new int[]{relId};
                }
            }
            propertyId = loadAnyProperty()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(property);
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
        });

        return buildT();
    }

    public CompletableFuture<T> buildDelayed(ExecutorService executorService) {
        return CompletableFuture.supplyAsync(this::build, executorService);
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     * If the label is not found, every node will be loaded.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public ME withLabel(String label) {
        this.label = Objects.requireNonNull(label);
        return me();
    }

    /**
     * Instructs the loader to load only nodes with the given {@link Label}.
     * If the label is not found, every node will be loaded.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public ME withLabel(Label label) {
        this.label = Objects.requireNonNull(label).name();
        return me();
    }

    /**
     * Instructs the loader to load any node with no restriction to any label.
     *
     * @return itself to enable fluent interface
     */
    public ME withAnyLabel() {
        this.label = null;
        return me();
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     * If the label is not found, every node will be loaded. TODO review that
     *
     * @param label May be null
     * @return itself to enable fluent interface
     */
    public ME withOptionalLabel(String label) {
        this.label = label;
        return me();
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     * If the type is not found, every relationship will be loaded.
     *
     * @param relationship May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public ME withRelationshipType(String relationship) {
        this.relationship = Objects.requireNonNull(relationship);
        return me();
    }

    /**
     * Instructs the loader to load only relationships with the given {@link RelationshipType}.
     * If the type is not found, every relationship will be loaded.
     *
     * @param relationship May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public ME withRelationshipType(RelationshipType relationship) {
        this.relationship = Objects.requireNonNull(relationship).name();
        return me();
    }

    /**
     * Instructs the loader to load any relationship with no restriction to any type.
     *
     * @return itself to enable fluent interface
     */
    public ME withAnyRelationshipType() {
        this.relationship = null;
        return me();
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     * If the argument is null, every relationship will be considered.
     *
     * @param relation May be null
     * @return itself to enable fluent interface
     */
    public ME withOptionalRelationshipType(String relation) {
        this.relationship = relation;
        return me();
    }


    /**
     * Instructs the loader to load weights by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property May not be null; to remove a weight property, use {@link #withoutWeights()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public ME withWeightsFromProperty(String property, double propertyDefaultValue) {
        this.property = Objects.requireNonNull(property);
        this.propertyDefaultValue = propertyDefaultValue;
        return me();
    }

    /**
     * Instructs the loader to load weights by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public ME withOptionalWeightsFromProperty(String property, double propertyDefaultValue) {
        this.property = property;
        this.propertyDefaultValue = propertyDefaultValue;
        return me();
    }

    /**
     * Instructs the loader to not load any weights. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public ME withDefaultWeight(double propertyDefaultValue) {
        this.property = null;
        this.propertyDefaultValue = propertyDefaultValue;
        return me();
    }

    /**
     * Instructs the loader to not load any weights. The behavior of using weighted graph-functions
     * on a graph without weights is not specified.
     *
     * @return itself to enable fluent interface
     */
    public ME withoutWeights() {
        this.property = null;
        this.propertyDefaultValue = DEFAULT_WEIGHT;
        return me();
    }

    /**
     * executes a consumer within its own transaction
     *
     * @param block the consumer
     */
    protected final void withinTransaction(Consumer<ReadOperations> block) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            block.accept(statement.readOperations());
            tx.success();
        }
    }

    /**
     * calls consumer for each NodeItem within an transaction
     *
     * @param consumer nodeItem consumer
     */
    protected void forEachNodeItem(Consumer<NodeItem> consumer) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            final ReadOperations readOp = statement.readOperations();
            if (labelId == ReadOperations.ANY_LABEL) {
                readOp.nodeCursorGetAll().forAll(consumer);
            } else {
                readOp.nodeCursorGetForLabel(labelId).forAll(consumer);
            }
            tx.success();
        }
    }

    /**
     * @return true if any relation should be considered, false otherwise
     */
    public boolean loadAnyRelationship() {
        return relationship == null;
    }

    /**
     * @return true if any label should be considered, false otherwise
     */
    public boolean loadAnyLabel() {
        return label == null;
    }

    /**
     * @return true if any property should be considered, false otherwise
     */
    public boolean loadAnyProperty() {
        return property == null;
    }

    /**
     * getThis-trick for method chaining in child classes
     * @return return self
     */
    protected abstract ME me();

    protected abstract T buildT();
}
