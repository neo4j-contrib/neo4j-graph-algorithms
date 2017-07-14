package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.logging.*;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    private Log log = NullLog.getInstance();

    private ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = log;
        this.progressLogger = new ProgressLoggerAdapter(log);
        return me();
    }

    public ProgressLogger getProgressLogger() {
        return progressLogger;
    }
}
