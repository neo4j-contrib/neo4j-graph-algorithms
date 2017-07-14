package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.logging.*;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    protected ProgressLogger log = ProgressLogger.NULL_LOGGER;

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = new ProgressLoggerAdapter(log);
        return me();
    }

    public ProgressLogger getProgressLog() {
        return log;
    }
}
