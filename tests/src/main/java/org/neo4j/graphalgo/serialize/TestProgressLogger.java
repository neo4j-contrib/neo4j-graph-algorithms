package org.neo4j.graphalgo.serialize;

import org.neo4j.graphalgo.core.utils.ProgressLogger;

import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class TestProgressLogger implements ProgressLogger {

    public static final ProgressLogger INSTANCE = new TestProgressLogger();

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        System.out.printf("%.0f%% (%s)%n", percentDone * 100, msg.get());
    }

    @Override
    public void logDone(Supplier<String> msg) {
        System.out.println(msg.get());
    }
}
