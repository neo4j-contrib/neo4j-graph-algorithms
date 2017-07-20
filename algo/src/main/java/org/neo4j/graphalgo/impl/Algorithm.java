package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.core.utils.TerminationFlagImpl;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.*;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    private static final String TASK = "COMPUTE";

    private Log log = NullLog.getInstance();

    private ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = log;
        this.progressLogger = new ProgressLoggerAdapter(log, TASK);
        return me();
    }

    public ME withKernelTransaction(KernelTransaction kernelTransaction) {
        this.terminationFlag = new TerminationFlagImpl(kernelTransaction);
        return me();
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }

    public ProgressLogger getProgressLogger() {
        return progressLogger;
    }

}
