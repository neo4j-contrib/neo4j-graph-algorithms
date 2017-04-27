package org.neo4j.graphalgo.api;

import java.util.concurrent.ExecutorService;

/**
 * DTO to ease the use of the GraphFactory-CTor. Should contain
 * setup options for loading the graph from neo4j.
 *
 * @author mknblch
 */
public class GraphSetup {

    // start label type. null means any label.
    public final String startLabel;
    // end label type (not yet implemented).
    public final String endLabel;
    // relationtype name. null means any relation.
    public final String relationshipType;
    // property of relationship weights. null means NO property (the default value will be used instead).
    public final String relationWeightPropertyName;
    // default property is used for weighted relationships if property is not set.
    public final double relationDefaultWeight;
    // property of node weights. null means NO property (the default value will be used instead).
    public final String nodeWeightPropertyName;
    // default property is used for weighted nodes if property is not set.
    public final double nodeDefaultWeight;
    // additional node property. null means NO property (the default value will be used instead).
    public final String nodePropertyName;
    // default property is used for node properties if property is not set.
    public final double nodeDefaultPropertyValue;

    // the executor service for parallel execution. null means single threaded evaluation.
    @Deprecated
    public final ExecutorService executor;
    /** statement to load nodes, has to return "id" and optionally "weight" or "value" */
    public final String nodeStatement;
    /** statement to load unique relationships, has to return ids of start "source" and end-node "target" and optionally "weight" */
    public final String relationshipStatement;

    /**
     * main ctor
     * @param startLabel the start label. null means any label.
     * @param endLabel not implemented yet
     * @param relationshipType the relation type identifier. null for any relationship
     * @param relationWeightPropertyName property name which holds the weights / costs of a relation.
     *                                   null means the default value is used for each weight.
     * @param relationDefaultWeight the default relationship weight if property is not given.
     * @param nodeWeightPropertyName property name which holds the weights / costs of a node.
     *                               null means the default value is used for each weight.
     * @param nodeDefaultWeight the default node weight if property is not given.
     * @param nodePropertyName property name which holds additional values of a node.
     *                         null means the default value is used for each value.
     * @param nodeDefaultPropertyValue the default node value if property is not given.
     * @param executor the executor. null means single threaded evaluation
     * @param nodeStatement statement to load nodes, has to return "id" and optionally "weight" or "value"
     * @param relationshipStatement statement to load unique relationships, has to return ids of start "source" and end-node "target" and optionally "weight"
     */
    public GraphSetup(
            String startLabel,
            String endLabel,
            String relationshipType,
            String relationWeightPropertyName,
            double relationDefaultWeight,
            String nodeWeightPropertyName,
            double nodeDefaultWeight,
            String nodePropertyName,
            double nodeDefaultPropertyValue,
            ExecutorService executor,
            String nodeStatement,
            String relationshipStatement) {

        this.startLabel = startLabel;
        this.endLabel = endLabel;
        this.relationshipType = relationshipType;
        this.relationWeightPropertyName = relationWeightPropertyName;
        this.relationDefaultWeight = relationDefaultWeight;
        this.nodeWeightPropertyName = nodeWeightPropertyName;
        this.nodeDefaultWeight = nodeDefaultWeight;
        this.nodePropertyName = nodePropertyName;
        this.nodeDefaultPropertyValue = nodeDefaultPropertyValue;
        this.executor = executor;
        this.nodeStatement = nodeStatement;
        this.relationshipStatement = relationshipStatement;
    }

    /**
     * Setup Graph to load any label, any relationship, no property in single threaded mode
     */
    public GraphSetup() {
        this.startLabel = null;
        this.endLabel = null;
        this.relationshipType = null;
        this.relationWeightPropertyName = null;
        this.relationDefaultWeight = 1.0;
        this.nodeWeightPropertyName = null;
        this.nodeDefaultWeight = 1.0;
        this.nodePropertyName = null;
        this.nodeDefaultPropertyValue = 1.0;
        this.executor = null;
        this.nodeStatement = null;
        this.relationshipStatement = null;
    }

    /**
     * Setup graph to load any label, any relationship, no property but
     * in multithreaded mode (depends on the actual executor)
     *
     * @param executor executor service
     */
    public GraphSetup(ExecutorService executor) {
        this.startLabel = null;
        this.endLabel = null;
        this.relationshipType = null;
        this.relationWeightPropertyName = null;
        this.relationDefaultWeight = 1.0;
        this.nodeWeightPropertyName = null;
        this.nodeDefaultWeight = 1.0;
        this.nodePropertyName = null;
        this.nodeDefaultPropertyValue = 1.0;
        this.executor = executor;
        this.nodeStatement = null;
        this.relationshipStatement = null;
    }

    public boolean loadConcurrent() {
        return executor != null;
    }

    public boolean loadDefaultRelationshipWeight() {
        return relationWeightPropertyName == null;
    }

    public boolean loadDefaultNodeWeight() {
        return nodeWeightPropertyName == null;
    }

    public boolean loadDefaultNodeProperty() {
        return nodePropertyName == null;
    }

    public boolean loadAnyLabel() {
        return startLabel == null;
    }

    public boolean loadAnyRelationshipType() {
        return relationshipType == null;
    }
}
