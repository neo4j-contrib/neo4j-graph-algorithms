package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class TriangleExporter extends ParallelExporter<TriangleExporter.Data> {

    private int coefficientPropertyId;

    public TriangleExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty) {
        super(db, idMapping, log, writeProperty);
    }

    public TriangleExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    public TriangleExporter setCoefficientWriteProperty(String property) {
        coefficientPropertyId = getOrCreatePropertyId(property);
        return this;
    }

    @Override
    protected void doWrite(DataWriteOperations writeOperations, Data data, int offset) throws KernelException {
        final long nodeId = idMapping.toOriginalNodeId(offset);
        writeOperations.nodeSetProperty(
                nodeId,
                DefinedProperty.longProperty(writePropertyId, data.triangles.get(offset)));
        writeOperations.nodeSetProperty(
                nodeId,
                DefinedProperty.doubleProperty(coefficientPropertyId, data.clusteringCoefficients[offset]));
    }

    public void write(AtomicIntegerArray triangles, double[] clusteringCoefficients) {
        write(new Data(triangles, clusteringCoefficients));
    }

    public static class Data {

        private final double[] clusteringCoefficients;
        private final AtomicIntegerArray triangles;

        public Data(AtomicIntegerArray triangles, double[] clusteringCoefficients) {
            this.clusteringCoefficients = clusteringCoefficients;
            this.triangles = triangles;
        }
    }
}
