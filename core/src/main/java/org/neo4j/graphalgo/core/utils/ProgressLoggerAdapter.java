package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mknblch
 */
public class ProgressLoggerAdapter implements ProgressLogger {

    private final Log log;

    private final String task;

    private int logInterval = 10_000; // 10s log interval by default

    private AtomicLong lastLog = new AtomicLong(0L);

    public ProgressLoggerAdapter(Log log, String task) {
        this.log = log;
        this.task = task;
    }

    @Override
    public void logProgress(double percentDone) {
        final long currentTime = System.currentTimeMillis();
        final long lastLogTime = lastLog.get();
        if (currentTime > lastLogTime + logInterval && lastLog.compareAndSet(lastLogTime, currentTime)) {
            log.info("[%s] %s %d%%", Thread.currentThread().getName(), task, (int) (percentDone * 100));
        }
    }

    @Override
    public void logDone() {
        log.info("[%s] %s %d%%", Thread.currentThread().getName(), task, 100);
    }

    public void withLogInterval(int logInterval) {
        this.logInterval = logInterval;
    }
}
