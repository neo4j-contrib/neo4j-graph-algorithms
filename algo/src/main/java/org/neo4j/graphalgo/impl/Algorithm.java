package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.ProgressLogAdapter;
import org.neo4j.logging.*;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    protected ProgressLogAdapter log = new ProgressLogAdapter(NullLog.getInstance());

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = new ProgressLogAdapter(log);
        return me();
    }

    public ProgressLogAdapter getProgressLog() {
        return log;
    }

}
