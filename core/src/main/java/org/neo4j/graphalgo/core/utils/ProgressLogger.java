package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

/**
 * @author mknblch
 */
public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = new ProgressLoggerAdapter(NullLog.getInstance(), "NULL");

    static ProgressLogger wrap(Log log, String task) {
        return new ProgressLoggerAdapter(log, task);
    }

    void logProgress(double percentDone);

    default void logProgress(double numerator, double denominator) {
        logProgress(numerator / denominator);
    }
}
