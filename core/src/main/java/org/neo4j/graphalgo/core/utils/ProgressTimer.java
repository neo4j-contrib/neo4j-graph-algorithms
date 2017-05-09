package org.neo4j.graphalgo.core.utils;


import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * @author mknobloch
 */
public class ProgressTimer implements AutoCloseable {

    private final LongConsumer onStop;
    private final long startTime;
    private long duration = 0;

    private ProgressTimer(LongConsumer onStop) {
        this.onStop = onStop == null ? l -> {} : onStop;
        startTime = System.nanoTime();
    }

    public ProgressTimer stop() {
        duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        onStop.accept(duration);
        return this;
    }

    public long getDuration() {
        return duration;
    }

    public static ProgressTimer start(LongConsumer onStop) {
        return new ProgressTimer(onStop);
    }

    public static ProgressTimer start() {
        return new ProgressTimer(null);
    }

    @Override
    public void close() {
        stop();
    }
}
