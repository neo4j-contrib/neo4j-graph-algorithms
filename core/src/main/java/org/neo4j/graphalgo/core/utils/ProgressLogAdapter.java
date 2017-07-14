package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;

/**
 * @author mknblch
 */
public class ProgressLogAdapter {

    private final Log log;

    private volatile int done = 0;

    public ProgressLogAdapter(Log log) {
        this.log = log;
    }

    public void resetProgress() {
        done = 0;
    }

    public void logProgress(double percentDone) {
        final int iPerc = (int) (percentDone * 100);
        if (iPerc > done) {
            log.info("[%s] %d%% done", Thread.currentThread().getName(), iPerc);
            done = iPerc;
        }
    }
}
