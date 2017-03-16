package org.neo4j.graphalgo;

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

    public void dump() {
        if (null == message) {
            System.out.printf("measurement took %dms%n", duration);
        } else {
            System.out.printf("%s took %dms%n", message, duration);
        }
    }

    public long getDuration() {
        return duration;
    }

    public static SimpleTimer start() {
        return new SimpleTimer(null);
    }

    public static SimpleTimer start(String message) {
        return new SimpleTimer(message);
    }

}
