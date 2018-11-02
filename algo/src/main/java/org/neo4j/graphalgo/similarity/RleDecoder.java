package org.neo4j.graphalgo.similarity;

import java.util.Arrays;

public class RleDecoder {
    private RleReader item1Reader = new RleReader(new double[0]);
    private RleReader item2Reader = new RleReader(new double[0]);
    private double[] item1Vector;
    private double[] item2Vector;

    public RleDecoder(int initialSize) {
        item1Vector = new double[initialSize];
        item2Vector = new double[initialSize];
    }

    public void reset(double[] item1, double[] item2) {
        item1Reader.reset(item1);
        item2Reader.reset(item2);
    }

    public double[] item1() {
        item1Reader.readInto(item1Vector);
        return item1Vector;
    }

    public double[] item2() {
        item2Reader.readInto(item2Vector);
        return item2Vector;
    }
}
