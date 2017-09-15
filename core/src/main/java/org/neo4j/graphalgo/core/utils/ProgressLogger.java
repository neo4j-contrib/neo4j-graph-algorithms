package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.function.Supplier;

/**
 * @author mknblch
 */
public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = new ProgressLoggerAdapter(NullLog.getInstance(), "NULL");
    Supplier<String> NO_MESSAGE = () -> null;

    static ProgressLogger wrap(Log log, String task) {
        return new ProgressLoggerAdapter(log, task);
    }

    void logProgress(double percentDone, Supplier<String> msg);

    void logDone(Supplier<String> msg);

    default void logProgress(double numerator, double denominator, Supplier<String> msg) {
        logProgress(numerator / denominator, msg);
    }
    default void logProgress(double numerator, double denominator) {
        logProgress(numerator, denominator, NO_MESSAGE);
    }
    default void logProgress(double percentDone) {
        logProgress(percentDone, NO_MESSAGE);
    }

    default void logDone() {
        logDone(NO_MESSAGE);
    }
}
