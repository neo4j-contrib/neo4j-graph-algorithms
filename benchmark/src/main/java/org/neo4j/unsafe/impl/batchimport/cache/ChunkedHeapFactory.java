package org.neo4j.unsafe.impl.batchimport.cache;

public abstract class ChunkedHeapFactory {
    private static final NumberArrayFactory FACTORY = new ChunkedNumberArrayFactory(NumberArrayFactory.HEAP);

    private ChunkedHeapFactory() {}

    public static DynamicLongArray newArray(int size, long defaultValue) {
        return (DynamicLongArray) FACTORY.newLongArray(size, defaultValue);
    }
}
