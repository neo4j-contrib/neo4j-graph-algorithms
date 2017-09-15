package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class ProgressLoggerAdapter implements ProgressLogger {

    private final Log log;

    private final String task;

    private int logIntervalMillis = 10_000; // 10s log interval by default

    private AtomicLong lastLog = new AtomicLong(0L);

    public ProgressLoggerAdapter(Log log, String task) {
        this.log = log;
        this.task = task;
    }

    @Override
    public void logProgress(double percentDone, Supplier<String> msgFactory) {
        final long currentTime = System.currentTimeMillis();
        final long lastLogTime = lastLog.get();
        if (currentTime > lastLogTime + logIntervalMillis && lastLog.compareAndSet(lastLogTime, currentTime)) {
            doLog((int) (percentDone * 100), msgFactory);
        }
    }

    @Override
    public void logDone(Supplier<String> msgFactory) {
        doLog(100, msgFactory);
    }

    public void withLogIntervalMillis(int logIntervalMillis) {
        this.logIntervalMillis = logIntervalMillis;
    }

    private void doLog(int percent, Supplier<String> msgFactory) {
        String message = msgFactory != ProgressLogger.NO_MESSAGE ? msgFactory.get() : null;
        if (message == null || message.isEmpty()) {
            log.info("[%s] %s %d%%", Thread.currentThread().getName(), task, percent);
        } else {
            log.info("[%s] %s %d%% %s", Thread.currentThread().getName(), task, percent, message);
        }
    }
}
