/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfByteArray;


public final class ByteArray extends PagedDataStructure<byte[]> {

    private final AtomicLong allocIdx = new PaddedAtomicLong();
    private final AllocationTracker tracker;

    private static final PageAllocator.Factory<byte[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(byte[].class, 1 << 18);


    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, ByteArray.class);
    }

    public static ByteArray newArray(long size, AllocationTracker tracker) {
        return new ByteArray(size, ALLOCATOR_FACTORY.newAllocator(tracker), tracker);
    }

    private ByteArray(long size, PageAllocator<byte[]> allocator, AllocationTracker tracker) {
        super(size, allocator);
        this.tracker = tracker;
    }

    public int getInt(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return getInt(pages[pageIndex], indexInPage);
    }

    private int getInt(byte[] page, int offset) {
        return ((page[offset] & 0xFF) << 24) |
                ((page[offset + 1] & 0xFF) << 16) |
                ((page[offset + 2] & 0xFF) << 8) |
                (page[offset + 3] & 0xFF);
    }

    public LocalAllocator newAllocator() {
        return new LocalAllocator(this);
    }

    /**
     * {@inheritDoc}
     */
    BulkAdder newBulkAdder() {
        return new BulkAdder(pages, pageSize, pageShift, pageMask);
    }

    /**
     * {@inheritDoc}
     */
    public DeltaCursor newCursor() {
        return new DeltaCursor(pages, pageShift, pageMask);
    }

    private long allocate(int numberOfPages, BulkAdder into) {
        long numberOfElements = capacityFor(numberOfPages);
        long intoIndex = allocIdx.getAndAdd(numberOfElements);
        grow(intoIndex + numberOfElements);
        into.grow(pages);
        into.init(intoIndex, numberOfPages);
        return intoIndex;
    }

    private long allocate(byte[] page, BulkAdder into) {
        long intoIndex = allocIdx.getAndAdd(pageSize);
        insertPage(intoIndex, page);
        tracker.add(sizeOfByteArray(page.length));
        into.insertPage(page);
        return intoIndex;
    }

    private void insertPage(final long atIndex, final byte[] newPage) {
        int pageIndex = pageIndex(atIndex);
        grow(atIndex + pageSize, pageIndex);
        pages[pageIndex] = newPage;
    }

    public final long release() {
        return super.release();
    }

    public DeltaCursor deltaCursor(DeltaCursor reuse, long offset) {
        return reuse.init(offset);
    }

    public static final class BulkAdder {

        private byte[][] pages;
        private final int pageShift;
        private final int pageMask;

        public byte[] array;
        public int offset;
        public final int limit;

        private int prevOffset;
        private int toPage;
        private int currentPage;

        private BulkAdder(
                byte[][] pages,
                int pageSize,
                int pageShift,
                int pageMask) {
            this.pages = pages;
            this.pageShift = pageShift;
            this.pageMask = pageMask;
            limit = pageSize;
            prevOffset = -1;
        }

        public void addUnsignedInt(int i) {
            offset = DeltaEncoding.encodeInt(i, array, offset);
        }

        public void addVLong(long i) {
            offset = DeltaEncoding.encodeVLong(i, array, offset);
        }

        void grow(byte[][] pages) {
            this.pages = pages;
        }

        void init(long fromIndex, int numberOfPages) {
            currentPage = PageUtil.pageIndex(fromIndex, pageShift);
            toPage = currentPage + numberOfPages - 1;
            array = pages[currentPage];
            offset = PageUtil.indexInPage(fromIndex, pageMask);
            assert offset == 0;
        }

        void insertPage(byte[] page) {
            if (prevOffset == -1) {
                prevOffset = offset;
            }
            array = page;
            offset = 0;
        }

        boolean reset() {
            if (prevOffset != -1) {
                array = pages[currentPage];
                offset = prevOffset;
                prevOffset = -1;
                return true;
            }
            return false;
        }

        public boolean next() {
            if (++currentPage <= toPage) {
                array = pages[currentPage];
                offset = 0;
                return true;
            }
            array = null;
            return false;
        }
    }

    public static final class LocalAllocator {
        private static final int PREFETCH_PAGES = 4;

        private final ByteArray array;

        public final BulkAdder adder;

        private long top;

        private LocalAllocator(final ByteArray array) {
            this.array = array;
            this.adder = array.newBulkAdder();
        }

        public void prepare() {
            top = array.allocate(PREFETCH_PAGES, adder);
            if (top == 0L) {
                ++top;
                ++adder.offset;
            }
        }

        public long allocate(long size) {
            return localAllocate(size, top);
        }

        private long localAllocate(long size, long address) {
            long maxOffset = array.pageSize - size;
            if (maxOffset >= adder.offset) {
                top += size;
                return address;
            }
            return majorAllocate(size, maxOffset, address);
        }

        private long majorAllocate(long size, long maxOffset, long address) {
            if (maxOffset < 0L) {
                return oversizingAllocate(size);
            }
            if (adder.reset() && maxOffset >= adder.offset) {
                top += size;
                return address;
            }
            address = top += (adder.limit - adder.offset);
            if (adder.next()) {
                // TODO: store and reuse fragments
                // branch: huge-alloc-fragmentation-recycle
                top += size;
                return address;
            }
            return prefetchAllocate(size);
        }

        private long prefetchAllocate(long size) {
            long address = top = array.allocate(PREFETCH_PAGES, adder);
            top += size;
            return address;
        }

        /**
         * We are faking a valid page by over-allocating a single page to be large enough to hold all data
         * Since we are storing all degrees into a single page and thus never have to switch pages
         * and keep the offsets as if this page would be of the correct size, we might just get by.
         */
        private long oversizingAllocate(long size) {
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("requested page of size " + size + " is too large to be allocated");
            }
            byte[] largePage = new byte[(int) size];
            return array.allocate(largePage, adder);
        }
    }

    public static final class DeltaCursor {

        private byte[][] pages;
        private final int pageShift;
        private final int pageMask;

        private byte[] array;
        private int offset;

        private int currentTarget;
        private int maxTargets;
        private long delta;

        private DeltaCursor(
                byte[][] pages,
                int pageShift,
                int pageMask) {
            this.pages = pages;
            this.pageShift = pageShift;
            this.pageMask = pageMask;
        }

        /**
         * Copy iteration state from another cursor without changing {@code other}.
         */
        public void copyFrom(DeltaCursor other) {
            array = other.array;
            offset = other.offset;
            currentTarget = other.currentTarget;
            maxTargets = other.maxTargets;
            delta = other.delta;
        }

        /**
         * Return how many targets can be decoded in total. This is equivalent to the degree.
         */
        public int cost() {
            return maxTargets;
        }

        /**
         * Return how many targets are still left to be decoded.
         */
        public int remaining() {
            return maxTargets - currentTarget;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        public boolean hasNextVLong() {
            return currentTarget < maxTargets;
        }

        /**
         * Read and decode the next target id.
         * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
         */
        public long nextVLong() {
            ++currentTarget;
            return nextVLong(array, offset);
        }

        /**
         * Read and decode target ids until it is strictly larger than (`>`) the provided {@code target}.
         * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        public long skipUntil(long target) {
            return skipUntil(target, array, offset);
        }

        /**
         * Read and decode target ids until it is larger than or equal (`>=`) the provided {@code target}.
         * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        public long advance(long target) {
            return advance(target, array, offset);
        }

        DeltaCursor init(long fromIndex) {
            initPage(fromIndex);

            currentTarget = 0;
            delta = 0L;
            initLength(array, offset);

            return this;
        }

        private void initPage(long fromIndex) {
            final int currentPage = PageUtil.pageIndex(fromIndex, pageShift);
            array = pages[currentPage];
            offset = PageUtil.indexInPage(fromIndex, pageMask);
        }

        private void initLength(byte[] array, int offset) {
            this.maxTargets = ((array[offset++] & 0xFF) << 24) |
                    ((array[offset++] & 0xFF) << 16) |
                    ((array[offset++] & 0xFF) << 8) |
                    (array[offset++] & 0xFF);
            this.offset = offset;
        }

        private long nextVLong(byte[] page, int offset) {
            byte b = page[offset++];
            long i = (long) ((int) b & 0x7F);
            for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                b = page[offset++];
                i |= ((long) b & 0x7FL) << shift;
            }
            this.offset = offset;
            return delta += i;
        }

        private long skipUntil(long target, byte[] page, int offset) {
            long value = delta;
            int current = currentTarget;
            int limit = maxTargets;
            while (value <= target && current++ < limit) {
                byte b = page[offset++];
                long i = (long) ((int) b & 0x7F);
                for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                    b = page[offset++];
                    i |= ((long) b & 0x7FL) << shift;
                }
                value += i;
            }
            this.currentTarget = current;
            this.offset = offset;
            this.delta = value;
            return value;
        }

        private long advance(long target, byte[] page, int offset) {
            long value = delta;
            int current = currentTarget;
            int limit = maxTargets;
            while (value < target && current++ < limit) {
                byte b = page[offset++];
                long i = (long) ((int) b & 0x7F);
                for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                    b = page[offset++];
                    i |= ((long) b & 0x7FL) << shift;
                }
                value += i;
            }
            this.currentTarget = current;
            this.offset = offset;
            this.delta = value;
            return value;
        }
    }
}
