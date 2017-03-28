package org.neo4j.graphalgo.api;

import java.util.concurrent.ExecutorService;

public class GraphSetup {

    public final String startLabel;
    public final String endLabel;
    public final String relationshipType;
    public final String propertyName;
    public final double propertyDefaultValue;
    public final ExecutorService executor;

    public GraphSetup(
            String startLabel,
            String endLabel,
            String relationshipType,
            String propertyName,
            double propertyDefaultValue,
            ExecutorService executor) {

        this.startLabel = startLabel;
        this.endLabel = endLabel;
        this.relationshipType = relationshipType;
        this.propertyName = propertyName;
        this.propertyDefaultValue = propertyDefaultValue;
        this.executor = executor;
    }

    public GraphSetup() {
        this.startLabel = null;
        this.endLabel = null;
        this.relationshipType = null;
        this.propertyName = null;
        this.propertyDefaultValue = 0.0;
        this.executor = null;
    }

    public GraphSetup(ExecutorService executor) {
        this.startLabel = null;
        this.endLabel = null;
        this.relationshipType = null;
        this.propertyName = null;
        this.propertyDefaultValue = 0.0;
        this.executor = executor;
    }

    public boolean loadConcurrent() {
        return executor != null;
    }

    public boolean loadAnyProperty() {
        return propertyName == null;
    }

    public boolean loadAnyLabel() {
        return startLabel == null;
    }

    public boolean loadAnyRelationshipType() {
        return relationshipType == null;
    }
}