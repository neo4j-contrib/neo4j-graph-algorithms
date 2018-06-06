package org.neo4j.values.storable;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

public class INDArrayPropertyTranslator implements PropertyTranslator<INDArray> {
    @Override
    public Value toProperty(int propertyId, INDArray data, long nodeId) {

        INDArray row = data.getRow((int) nodeId);

        double[] rowAsDouble = new double[row.size(1)];
        for (int columnIndex = 0; columnIndex < row.size(1); columnIndex++) {
            rowAsDouble[columnIndex] = row.getDouble(columnIndex);
        }

        return new DoubleArray(rowAsDouble);
    }
}
