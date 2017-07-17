package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.NullLog;

/**
 * @author mknblch
 */
public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = new ProgressLoggerAdapter(NullLog.getInstance(), "NULL");

    void logProgress(double percentDone);

    default void logProgress(int numerator, int denominator) {
        logProgress((double) numerator / denominator);
    }
}
