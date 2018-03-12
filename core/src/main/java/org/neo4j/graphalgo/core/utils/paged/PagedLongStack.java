package org.neo4j.graphalgo.core.utils.paged;

/**
 * @author mknblch
 */
public class PagedLongStack extends PagedDataStructure<long[]> {

    private static final PageAllocator.Factory<long[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(long[].class);

    public PagedLongStack(long initialSize, AllocationTracker tracker) {
        this(Math.max(1L, initialSize), ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private PagedLongStack(long initialSize, PageAllocator<long[]> allocator) {
        super(initialSize, allocator);
        clear();
    }

    private long size;
    private int pageIndex;
    private int pageTop;
    private int pageLimit;
    private long[] currentPage;

    public void clear() {
        size = 0L;
        pageTop = -1;
        pageIndex = 0;
        currentPage = pages[0];
        pageLimit = currentPage.length;
    }

    public void push(long value) {
        int pageTop = ++this.pageTop;
        if (pageTop >= pageLimit) {
            pageTop = nextPage();
        }
        ++size;
        currentPage[pageTop] = value;
    }

    public long pop() {
        int pageTop = this.pageTop;
        if (pageTop < 0) {
            pageTop = previousPage();
        }
        --this.pageTop;
        --size;
        return currentPage[pageTop];
    }

    public long peek() {
        return currentPage[pageTop];
    }

    public boolean isEmpty() {
        return size == 0L;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long release() {
        long released = super.release();
        size = 0L;
        pageTop = 0;
        pageIndex = 0;
        pageLimit = 0;
        currentPage = null;
        return released;
    }

    private int nextPage() {
        int pageIndex = ++this.pageIndex;
        if (pageIndex >= pages.length) {
            grow(capacityFor(pageIndex + 1));
        }
        currentPage = pages[pageIndex];
        pageLimit = currentPage.length;
        return pageTop = 0;
    }

    private int previousPage() {
        int pageIndex = this.pageIndex;
        --pageIndex;
        // let it throw
        currentPage = pages[pageIndex];
        pageLimit = currentPage.length;
        this.pageIndex = pageIndex;
        return pageTop = pageLimit - 1;
    }
}
