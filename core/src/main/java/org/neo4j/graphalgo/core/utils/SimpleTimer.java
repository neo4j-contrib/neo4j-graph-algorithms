package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.TimeUnit;

/**
 * @author mknobloch
 */
public class SimpleTimer {

    private final String message;
    private final long startTime;
    private long duration = 0;

    private SimpleTimer(String message) {
        this.message = message;
        startTime = System.nanoTime();
    }

    public SimpleTimer stop() {
        duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        return this;
    }

    public void print() {
        if (null == message) {
            System.out.printf("measurement took %d ms%n", duration);
        } else {
            System.out.printf("%s took %d ms%n", message, duration);
        }
    }

    public long getDuration() {
        return duration;
    }

    public static SimpleTimer start(String message) {
        return new SimpleTimer(message);
    }

}
