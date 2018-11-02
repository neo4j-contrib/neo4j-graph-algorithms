package org.neo4j.graphalgo.similarity;

import java.util.Arrays;

public class RleDecoder {
    private RleReader item1Reader = new RleReader(new double[0]);
    private RleReader item2Reader = new RleReader(new double[0]);
    private double[] item1Vector;
    private double[] item2Vector;
    private int initialSize;
    private boolean recomputeItem1Vector = true;
    private boolean recomputeItem2Vector = true;

    public RleDecoder(int initialSize) {
        this.initialSize = initialSize;
        item1Vector = new double[initialSize];
        item2Vector = new double[initialSize];
    }

    public void reset(double[] item1, double[] item2) {
        if(!item1.equals(item1Reader.vector())) {
            item1Reader.reset(item1);
            Arrays.fill(item1Vector, 0);
            recomputeItem1Vector = true;
        } else {
            recomputeItem1Vector = false;
        }

        if(!item2.equals(item2Reader.vector())) {
            item2Reader.reset(item2);
            Arrays.fill(item2Vector, 0);
            recomputeItem2Vector = true;
        } else {
            recomputeItem2Vector = false;
        }
    }

    public double[] item1() {
        if (recomputeItem1Vector) {
            decode(item1Reader, item1Vector);
        }

        return item1Vector;
    }

    public double[] item2() {
        if (recomputeItem2Vector) {
            decode(item2Reader, item2Vector);
        }
        return item2Vector;
    }

    private void decode(RleReader reader, double[] vector) {
        for (int i = 0; i < initialSize; i++) {
            reader.next();
            vector[i] = reader.value();
        }
    }
}
