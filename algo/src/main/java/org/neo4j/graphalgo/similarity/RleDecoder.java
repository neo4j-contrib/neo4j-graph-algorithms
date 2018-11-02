package org.neo4j.graphalgo.similarity;

public class RleDecoder {
    private RleReader item1Reader;
    private RleReader item2Reader;

    public RleDecoder(int initialSize) {
        item1Reader = new RleReader(initialSize);
        item2Reader = new RleReader(initialSize);
    }

    public void reset(double[] item1, double[] item2) {
        item1Reader.reset(item1);
        item2Reader.reset(item2);
    }

    public double[] item1() {
        return item1Reader.read();
    }

    public double[] item2() {
        return item2Reader.read();
    }
}
