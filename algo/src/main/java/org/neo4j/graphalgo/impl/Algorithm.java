package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.CancellationFlag;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.logging.*;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    private static final String TASK = "COMPUTE";

    private Log log = NullLog.getInstance();

    private ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    private CancellationFlag cancellationFlag;

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = log;
        this.progressLogger = new ProgressLoggerAdapter(log, TASK);
        return me();
    }

    public CancellationFlag getCancellationFlag() {
        return cancellationFlag;
    }

    public ProgressLogger getProgressLogger() {
        return progressLogger;
    }

    public Algorithm withCancellationFlag(CancellationFlag cancellationFlag) {
        this.cancellationFlag = cancellationFlag;
        return this;
    }
}
