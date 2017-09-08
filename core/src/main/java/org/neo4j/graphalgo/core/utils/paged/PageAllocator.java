package org.neo4j.graphalgo.core.utils.paged;

import java.lang.reflect.Array;
import java.util.function.Supplier;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

public abstract class PageAllocator<T> {

    public abstract T newPage();

    public abstract int pageSize();

    public abstract T[] emptyPages();

    public abstract long bytesPerPage();

    public final long estimateMemoryUsage(long size) {
        long numPages = PageUtil.numPagesFor(size, pageSize());
        return numPages * bytesPerPage();
    }

    public static <T> Factory<T> of(
            int pageSize,
            long bytesPerPage,
            Supplier<T> newPage,
            T[] emptyPages) {
        return new Factory<>(pageSize, bytesPerPage, newPage, emptyPages);
    }

    @SuppressWarnings("unchecked")
    public static <T> Factory<T> ofArray(Class<T> arrayClass) {
        Class<?> componentType = arrayClass.getComponentType();
        assert componentType != null && componentType.isPrimitive();

        long bytesPerElement = shallowSizeOfInstance(componentType);
        int pageSize = PageUtil.pageSizeFor((int) bytesPerElement);

        T[] emptyPages = (T[]) Array.newInstance(componentType, 0, 0);
        Supplier<T> newPage = () -> (T) Array.newInstance(componentType, pageSize);

        long bytesPerPage = alignObjectSize(NUM_BYTES_ARRAY_HEADER + pageSize * bytesPerElement);
        return of(pageSize, bytesPerPage, newPage, emptyPages);
    }

    public static final class Factory<T> {
        private final int pageSize;
        private final long bytesPerPage;
        private final Supplier<T> newPage;

        private final T[] emptyPages;

        private Factory(
                int pageSize,
                long bytesPerPage,
                Supplier<T> newPage,
                T[] emptyPages) {
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
            this.newPage = newPage;
            this.emptyPages = emptyPages;
        }

        public long estimateMemoryUsage(long size) {
            long numPages = PageUtil.numPagesFor(size, pageSize);
            return numPages * bytesPerPage;
        }

        public long estimateMemoryUsage(long size, Class<?> container) {
            return shallowSizeOfInstance(container) + estimateMemoryUsage(size);
        }

        PageAllocator<T> newAllocator(AllocationTracker tracker) {
            if (AllocationTracker.isTracking(tracker)) {
                return new TrackingAllocator<>(
                        newPage,
                        emptyPages,
                        pageSize,
                        bytesPerPage,
                        tracker);
            }
            return new DirectAllocator<>(newPage, emptyPages, pageSize, bytesPerPage);
        }
    }

    private static final class TrackingAllocator<T> extends PageAllocator<T> {

        private final Supplier<T> newPage;
        private final T[] emptyPages;
        private final int pageSize;
        private final long bytesPerPage;
        private final AllocationTracker tracker;

        private TrackingAllocator(
                Supplier<T> newPage,
                T[] emptyPages,
                int pageSize,
                long bytesPerPage,
                AllocationTracker tracker) {
            this.emptyPages = emptyPages;
            assert BitUtil.isPowerOfTwo(pageSize);
            this.newPage = newPage;
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
            this.tracker = tracker;
        }

        @Override
        public T newPage() {
            tracker.add(bytesPerPage);
            return newPage.get();
        }

        @Override
        public int pageSize() {
            return pageSize;
        }

        @Override
        public long bytesPerPage() {
            return bytesPerPage;
        }

        @Override
        public T[] emptyPages() {
            return emptyPages;
        }
    }

    private static final class DirectAllocator<T> extends PageAllocator<T> {

        private final Supplier<T> newPage;
        private final T[] emptyPages;
        private final int pageSize;
        private final long bytesPerPage;

        private DirectAllocator(
                Supplier<T> newPage,
                T[] emptyPages,
                int pageSize,
                long bytesPerPage) {
            assert BitUtil.isPowerOfTwo(pageSize);
            this.emptyPages = emptyPages;
            this.newPage = newPage;
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
        }

        @Override
        public T newPage() {
            return newPage.get();
        }

        @Override
        public int pageSize() {
            return pageSize;
        }

        @Override
        public long bytesPerPage() {
            return bytesPerPage;
        }

        @Override
        public T[] emptyPages() {
            return emptyPages;
        }
    }
}

