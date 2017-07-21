package org.neo4j.graphalgo.core.utils;

import org.neo4j.kernel.api.KernelTransaction;

/**
 * @author mknblch
 */
public interface TerminationFlag {

    TerminationFlag RUNNING_TRUE = () -> true;

    static TerminationFlag wrap(KernelTransaction transaction) {
        return new TerminationFlagImpl(transaction);
    }

    boolean running();
}
