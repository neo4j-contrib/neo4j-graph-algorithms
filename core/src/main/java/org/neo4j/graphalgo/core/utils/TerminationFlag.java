package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public interface TerminationFlag {

    TerminationFlag ALWAYS_TRUE = () -> true;

    boolean running();

}
