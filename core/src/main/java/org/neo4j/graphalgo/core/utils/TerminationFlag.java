package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public interface TerminationFlag {

    TerminationFlag RUNNING_TRUE = () -> true;

    boolean running();

}
