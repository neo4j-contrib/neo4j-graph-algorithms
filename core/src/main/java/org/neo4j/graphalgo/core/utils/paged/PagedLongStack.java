package org.neo4j.graphalgo.core.utils.paged;

/**
 * @author mknblch
 */
public class PagedLongStack {

    private final LongArray array;
    private long offset;

    public PagedLongStack(long size, AllocationTracker tracker) {
        array = LongArray.newArray(size, tracker);
    }

    public void clear() {
        offset = 0;
    }

    public void push(long value) {
        array.set(offset++, value);
    }

    public long pop() {
        return array.get(--offset);
    }

    public long peek() {
        return array.get(offset - 1);
    }

    public boolean isEmpty() {
        return offset == 0;
    }

    public long size() {
        return offset;
    }
}
