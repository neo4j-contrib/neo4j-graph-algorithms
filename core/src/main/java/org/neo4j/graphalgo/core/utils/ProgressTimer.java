package org.neo4j.graphalgo.core.utils;


import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
public class ProgressTimer {

    private final Consumer<Long> onStop;
    private final long startTime;
    private long duration = 0;

    private ProgressTimer(Consumer<Long> onStop) {
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

    public static ProgressTimer start(Consumer<Long> onStop) {
        return new ProgressTimer(onStop);
    }

    public static ProgressTimer start() {
        return new ProgressTimer(null);
    }

}
