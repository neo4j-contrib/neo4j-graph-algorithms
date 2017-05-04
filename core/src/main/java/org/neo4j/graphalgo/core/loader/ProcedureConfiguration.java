package org.neo4j.graphalgo.core.loader;

import org.neo4j.logging.Log;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author mknblch
 */
public class ProcedureConfiguration {

    private Pattern QUERY_PATTERN = Pattern.compile("MATCH.+RETURN.+", Pattern.CASE_INSENSITIVE);

    private final Map<String, Object> config;

    private final Log log;

    public ProcedureConfiguration(Map<String, Object> config, Log log) {
        this.config = config;
        this.log = log;
    }

    public ProcedureConfiguration withNodeLabelOrQuery(String nodeLabelOrQuery) {
        config.put(ProcedureConstants.NODE_LABEL_QUERY_PARAM, nodeLabelOrQuery);
        return this;
    }

    public ProcedureConfiguration withRelationshipTypeOrQuery(String relationshipTypeOrQuery) {
        config.put(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, relationshipTypeOrQuery);
        return this;
    }

    public ProcedureConfiguration withProperty(String property) {
        config.put(ProcedureConstants.PROPERTY_PARAM, property);
        return this;
    }

    public ProcedureConfiguration withDirection(String direction) {
        config.put(ProcedureConstants.DIRECTION, direction);
        return this;
    }

    public boolean nodeLabelIsQuery() {
        return QUERY_PATTERN
                .matcher(getNodeLabelOrQuery(""))
                .matches();
    }

    public String getNodeLabelOrQuery() {
        return getStringOrNull(ProcedureConstants.NODE_LABEL_QUERY_PARAM, null);
    }

    public String getNodeLabelOrQuery(String defaultValue) {
        return getStringOrNull(ProcedureConstants.NODE_LABEL_QUERY_PARAM, defaultValue);
    }

    public boolean relationshipIsQuery() {
        return QUERY_PATTERN
                .matcher(getRelationshipOrQuery(""))
                .matches();
    }

    public String getRelationshipOrQuery() {
        return getStringOrNull(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, null);
    }

    public String getRelationshipOrQuery(String defaultValue) {
        return getStringOrNull(ProcedureConstants.RELATIONSHIP_QUERY_PARAM, defaultValue);
    }

    public boolean isWriteFlag() {
        return get(ProcedureConstants.WRITE_FLAG, Boolean.FALSE);
    }

    public String getWriteProperty() {
        return getStringOrNull(ProcedureConstants.WRITE_PROPERTY, ProcedureConstants.WRITE_PROPERTY_DEFAULT);
    }

    public boolean isStatsFlag() {
        return get(ProcedureConstants.STATS_FLAG, Boolean.FALSE);
    }

    public String getProperty() {
        return getStringOrNull(ProcedureConstants.PROPERTY_PARAM, null);
    }

    public double getPropertyDefaultValue(double defaultValue) {
        return get(ProcedureConstants.DEFAULT_PROPERTY_VALUE_PARAM, defaultValue);
    }

    public int getIterations() {
        return getNumber(ProcedureConstants.ITERATIONS_PARAM, ProcedureConstants.ITERATIONS_DEFAULT);
    }

    public String getDirectionName() {
        return get(ProcedureConstants.DIRECTION, ProcedureConstants.DIRECTION_DEFAULT);
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
        Object value = config.get(key);
        if (null == value) {
            log.warn("Procedure configuration for key: " + key + " not found! Using defaultValue: " + defaultValue);
            return defaultValue;
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("The value of " + key + " must String type");
        }
        log.debug("Using configuration " + key + ":" + value);
        return emptyToNull((String) value);
    }

    @SuppressWarnings("unchecked")
    public <V extends Number> V getNumber(String key, V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            log.warn("Procedure configuration for key: " + key + " not found! Using defaultValue: " + defaultValue);
            return defaultValue;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The value of " + key + " must Number type");
        }
        log.debug("Using configuration " + key + ":" + value);
        return (V) value;
    }

    private String emptyToNull(String s) {
        return "".equals(s) ? null : s;
    }

    @SuppressWarnings("unchecked")
    private <V> V get(String key, V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            log.warn("Procedure configuration for key: " + key + " not found! Using defaultValue: " + defaultValue);
            return defaultValue;
        }
        log.debug("Procedure configuration found for key: " + key);
        return (V) value;
    }

    public static ProcedureConfiguration create(Log log, Map<String, Object> config) {
        return new ProcedureConfiguration(config, log);
    }

    public static ProcedureConfiguration create(Log log, Map<String, Object> config, String nodeLabelOrQuery) {
        return new ProcedureConfiguration(config, log)
                .withNodeLabelOrQuery(nodeLabelOrQuery);
    }

    public static ProcedureConfiguration create(Log log, Map<String, Object> config, String nodeLabelOrQuery, String relationshipOrQuery) {
        return new ProcedureConfiguration(config, log)
                .withNodeLabelOrQuery(nodeLabelOrQuery)
                .withRelationshipTypeOrQuery(relationshipOrQuery);
    }

    public static ProcedureConfiguration create(Log log, Map<String, Object> config, String nodeLabelOrQuery, String relationshipOrQuery, String property) {
        return new ProcedureConfiguration(config, log)
                .withNodeLabelOrQuery(nodeLabelOrQuery)
                .withRelationshipTypeOrQuery(relationshipOrQuery)
                .withProperty(property);
    }


}
