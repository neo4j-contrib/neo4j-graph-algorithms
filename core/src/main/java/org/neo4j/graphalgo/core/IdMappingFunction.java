package org.neo4j.graphalgo.core;

@FunctionalInterface
public interface IdMappingFunction {
    int mapId(long nodeId);
}
