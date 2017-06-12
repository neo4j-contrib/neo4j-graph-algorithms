package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @author mknblch
 */
public class ProcedureConfiguration {

    private final Map<String, Object> config;

    public ProcedureConfiguration(Map<String, Object> config) {
        this.config = new HashMap<>(config);
    }

    public boolean containsKeys(String... keys) {
        for (String key : keys) {
            if (!config.containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * override the nodeOrLabelQuery param
     * @param nodeLabelOrQuery the query or identifier
     * @return self
     */
    public ProcedureConfiguration overrideNodeLabelOrQuery(String nodeLabelOrQuery) {
        config.put(ProcedureConstants.NODE_LABEL_QUERY_PARAM, nodeLabelOrQuery);
        return this;
    }

    /**
     * override relationshipOrQuery param
     * @param relationshipTypeOrQuery the relationshipQuery or Identifier
     * @return self
     */
    public ProcedureConfiguration overrideRelationshipTypeOrQuery(String relationshipTypeOrQuery) {
        config.put(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, relationshipTypeOrQuery);
        return this;
    }

    /**
     * override property param
     * @param weightProperty the property
     * @return self
     */
    public ProcedureConfiguration overrideProperty(String weightProperty) {
        config.put(ProcedureConstants.PROPERTY_PARAM, weightProperty);
        return this;
    }

    public String getNodeLabelOrQuery() {
        return getStringOrNull(ProcedureConstants.NODE_LABEL_QUERY_PARAM, null);
    }

    public String getNodeLabelOrQuery(String defaultValue) {
        return getStringOrNull(ProcedureConstants.NODE_LABEL_QUERY_PARAM, defaultValue);
    }

    public String getRelationshipOrQuery() {
        return getStringOrNull(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, null);
    }

    public String getWriteProperty() {
        return getWriteProperty(null);
    }

    public String getWriteProperty(String defaultValue) {
        return getStringOrNull(ProcedureConstants.WRITE_PROPERTY, defaultValue);
    }

    public String getRelationshipOrQuery(String defaultValue) {
        return getStringOrNull(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, defaultValue);
    }

    public boolean isWriteFlag() {
        return isWriteFlag(false);
    }

    public boolean isCypherFlag() {
        return isCypherFlag(false);
    }

    public boolean isStatsFlag() {
        return isStatsFlag(false);
    }

    public boolean isWriteFlag(boolean defaultValue) {
        return get(ProcedureConstants.WRITE_FLAG, defaultValue);
    }

    public boolean isCypherFlag(boolean defaultValue) {
        return (boolean) config.getOrDefault(ProcedureConstants.CYPHER_QUERY, defaultValue);
    }

    public boolean isStatsFlag(boolean defaultValue) {
        return get(ProcedureConstants.STATS_FLAG, defaultValue);
    }

    public String getProperty() {
        return getStringOrNull(ProcedureConstants.PROPERTY_PARAM, null);
    }

    public double getPropertyDefaultValue(double defaultValue) {
        return get(ProcedureConstants.DEFAULT_PROPERTY_VALUE_PARAM, defaultValue);
    }

    public int getIterations(int defaultValue) {
        return getNumber(ProcedureConstants.ITERATIONS_PARAM, defaultValue).intValue();
    }

    public int getBatchSize() {
        return getNumber(ProcedureConstants.BATCH_SIZE_PARAM, ParallelUtil.DEFAULT_BATCH_SIZE).intValue();
    }

    public int getBatchSize(int defaultValue) {
        return getNumber(ProcedureConstants.BATCH_SIZE_PARAM, defaultValue).intValue();
    }

    public int getConcurrency(int defaultValue) {
        return getNumber(ProcedureConstants.CONCURRENCY, defaultValue).intValue();
    }

    public int getConcurrency() {
        return getConcurrency(ProcedureConstants.DEFAULT_CONCURRENCY);
    }

    public String getDirectionName() {
        return get(ProcedureConstants.DIRECTION, ProcedureConstants.DIRECTION_DEFAULT);
    }

    public Class<? extends GraphFactory> getGraphImpl() {
        final String graphImpl = getStringOrNull(
                ProcedureConstants.GRAPH_IMPL_PARAM,
                ProcedureConstants.DEFAULT_GRAPH_IMPL);
        switch (graphImpl.toLowerCase(Locale.ROOT)) {
            case "heavy":
                return HeavyGraphFactory.class;
            case "cypher":
                return HeavyCypherGraphFactory.class;
            case "light":
                return LightGraphFactory.class;
            case "kernel":
                return GraphViewFactory.class;
            default:
                throw new IllegalArgumentException("Unknown impl: " + graphImpl);
        }
    }

    /**
     * specialized getter for String which either returns the value
     * if found, the defaultValue if the key is not found or null if
     * the key is found but its value is empty.
     *
     * @param key configuration key
     * @param defaultValue the default value if key is not found
     * @return the configuration value
     */
    public String getStringOrNull(String key, String defaultValue) {
        String value = (String) config.getOrDefault(key, defaultValue);
        return (null == value || "".equals(value)) ? defaultValue : value;
    }

    public Object get(String key) {
        return config.get(key);
    }

    @SuppressWarnings("unchecked")
    public Number getNumber(String key, Number defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The value of " + key + " must Number type");
        }
        return (Number) value;
    }

    public int getInt(String key, int defaultValue) {
        Number value = (Number) config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return value.intValue();
    }

    @SuppressWarnings("unchecked")
    public <V> V get(String key, V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return (V) value;
    }

    public static ProcedureConfiguration create(Map<String, Object> config) {
        return new ProcedureConfiguration(config);
    }
}
