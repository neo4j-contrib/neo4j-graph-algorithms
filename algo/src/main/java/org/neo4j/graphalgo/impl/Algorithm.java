package org.neo4j.graphalgo.impl;

import org.neo4j.logging.*;

import java.io.PrintWriter;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> {

    private final AtomicInteger percentDone = new AtomicInteger(0);

    private boolean logEnabled = false;

    protected Log log = new SysOutLogger("System");

    public abstract ME me();

    public ME withLog(Log log) {
        this.log = log;
        return me();
    }

    public ME withLogEnabled(boolean logEnabled) {
        this.logEnabled = logEnabled;
        return me();
    }

    public Log getLog() {
        return log;
    }

    public ME resetProgress() {
        percentDone.set(0);
        return me();
    }

    protected void logProgress(double percent) {
        if (!logEnabled) {
            return;
        }
        final int iPerc = (int) (percent * 100.0);
        final int current = percentDone.get();
        if (iPerc > current && percentDone.compareAndSet(current, iPerc)) {
            log.info("[%s] %d%% done", Thread.currentThread().getName(), iPerc);
        }
    }

    private static class SysOutLogger extends FormattedLog {

        public SysOutLogger(String category) {
            this(Date::new,
                    () -> new PrintWriter(System.out),
                    TimeZone.getDefault(),
                    new Object(),
                    category,
                    Level.DEBUG,
                    true);
        }

        protected SysOutLogger(Supplier<Date> currentDateSupplier,
                               Supplier<PrintWriter> writerSupplier,
                               TimeZone timezone,
                               Object maybeLock,
                               String category,
                               Level level,
                               boolean autoFlush) {
            super(currentDateSupplier, writerSupplier, timezone, maybeLock, category, level, autoFlush);
        }
    }
}
