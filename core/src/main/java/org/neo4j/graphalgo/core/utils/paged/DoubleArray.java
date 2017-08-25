package org.neo4j.graphalgo.core.utils.paged;

import java.util.Arrays;

public final class DoubleArray extends PagedDataStructure<double[]> {

    public static DoubleArray newArray(long size) {
        return new DoubleArray(size);
    }

    private DoubleArray(long size) {
        super(size, Double.BYTES, double[].class);
    }

    public double get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public double set(long index, double value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final double[] page = pages[pageIndex];
        final double ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    @Override
    protected double[] newPage() {
        return new double[pageSize];
    }

    public void fill(double value) {
        for (double[] page : pages) {
            Arrays.fill(page, value);
        }
    }
}
