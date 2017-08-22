package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> implements TerminationFlag {

    private ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public abstract ME me();

    public abstract ME release();

    public ME withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return me();
    }

    public ME withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return me();
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }

    public ProgressLogger getProgressLogger() {
        return progressLogger;
    }

    @Override
    public boolean running() {
        return terminationFlag.running();
    }
}
