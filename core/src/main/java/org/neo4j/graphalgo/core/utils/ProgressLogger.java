package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.NullLog;

import java.util.function.DoubleConsumer;
import java.util.function.DoubleSupplier;

/**
 * @author mknblch
 */
public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = new ProgressLoggerAdapter(NullLog.getInstance());

    void logProgress(double percentDone);

}
