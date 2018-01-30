package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;

/**
 * @author mknblch
 */
public class PagedSimpleBitSet extends PagedDataStructure<SimpleBitSet> {

    private static final PageAllocator.Factory<SimpleBitSet> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(Double.BYTES);
        long pageUsage = shallowSizeOfInstance(SimpleBitSet.class) + sizeOfLongArray(pageSize);

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new SimpleBitSet(pageSize),
                new SimpleBitSet[0]);
    }

    public static PagedSimpleBitSet newBitSet(long size, AllocationTracker tracker) {
        return new PagedSimpleBitSet(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    PagedSimpleBitSet(long size, PageAllocator<SimpleBitSet> allocator) {
        super(size, allocator);
    }

    public void put(long value) {
        final int pageIndex = pageIndex(value);
        final int indexInPage = indexInPage(value);
        pages[pageIndex].put(indexInPage);
    }

    public boolean contains(long value) {
        final int pageIndex = pageIndex(value);
        final int indexInPage = indexInPage(value);
        return pages[pageIndex].contains(indexInPage);
    }

    public void remove(long value) {
        final int pageIndex = pageIndex(value);
        final int indexInPage = indexInPage(value);
        pages[pageIndex].remove(indexInPage);

    }

    public void clear() {
        final int pages = numPages(capacity());
        for (int i = 0; i < pages; i++) {
            this.pages[i].clear();
        }
    }

    /**
     * this hurts
     */
    public long _size() {
        long size = 0;
        final int pages = numPages(capacity());
        for (int i = 0; i < pages; i++) {
            size += this.pages[i].size();
        }
        return size;
    }
}
