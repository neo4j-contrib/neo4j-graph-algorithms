package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface NodeProperties {

    double valueOf(int nodeId, double defaultValue);
}
