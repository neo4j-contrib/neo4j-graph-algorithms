package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;

/**
 * @author mknblch
 */
public class ProgressLoggerAdapter implements ProgressLogger {

    private final Log log;

    private volatile int done = 0;

    public ProgressLoggerAdapter(Log log) {
        this.log = log;
    }

    @Override
    public void resetProgress() {
        done = 0;
    }

    @Override
    public void logProgress(double percentDone) {
        final int iPerc = (int) (percentDone * 100);
        if (iPerc > done) {
            log.info("[%s] %d%% done", Thread.currentThread().getName(), iPerc);
            done = iPerc;
        }
    }
}
