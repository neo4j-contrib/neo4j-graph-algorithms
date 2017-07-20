package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public interface CancellationFlag {

    boolean running();

    static CancellationFlag getInstance() {
        return () -> true;
    }
}
