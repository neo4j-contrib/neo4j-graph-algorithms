package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

public interface PageRankResult {

    long size();

    double score(int nodeId);

    double score(long nodeId);

    AbstractExporter<PageRankResult> exporter(
            GraphDatabaseAPI db,
            Log log,
            String writeProperty,
            ExecutorService executorService,
            int concurrency);

    default boolean hasFastToDoubleArray() {
        return false;
    }

    default double[] toDoubleArray() {
        int size = Math.toIntExact(size());
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = score(i);
        }
        return result;
    }
}
