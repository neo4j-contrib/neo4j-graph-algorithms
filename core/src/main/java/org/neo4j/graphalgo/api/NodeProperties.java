package org.neo4j.graphalgo.api;

/**
 * Getter interface for node properties.
 *
 * @author mknblch
 */
public interface NodeProperties {

    /**
     * return the property value for a node
     *
     * @param nodeId       the node id
     * @param defaultValue a default value
     * @return the property value
     */
    double valueOf(int nodeId, double defaultValue);
}
