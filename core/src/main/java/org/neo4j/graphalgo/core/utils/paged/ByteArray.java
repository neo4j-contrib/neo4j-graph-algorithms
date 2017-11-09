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


public final class ByteArray extends PagedDataStructure<byte[]> {

    private final AtomicLong allocIdx = new PaddedAtomicLong();

    private static final PageAllocator.Factory<byte[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(byte[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, ByteArray.class);
    }

    public static ByteArray newArray(long size, AllocationTracker tracker) {
        return new ByteArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private ByteArray(long size, PageAllocator<byte[]> allocator) {
        super(size, allocator);
    }

    public byte get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public int getInt(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        byte[] page = pages[pageIndex];

        if (page.length - indexInPage >= 4) {
            return getInt(page, indexInPage);
        }
        if (pageIndex + 1 >= pages.length) {
            return -1;
        }
        return getInt(page, pages[pageIndex + 1], indexInPage);
    }

    private int getInt(byte[] page, int offset) {
        return ((page[offset] & 0xFF) << 24) |
                ((page[offset + 1] & 0xFF) << 16) |
                ((page[offset + 2] & 0xFF) << 8) |
                (page[offset + 3] & 0xFF);
    }

    private int getInt(byte[] page, byte[] nextPage, int offset) {
        switch (page.length - offset) {
            case 0:
                return getInt(nextPage, 0);
            case 1:
                return ((page[offset] & 0xFF) << 24) |
                        ((nextPage[0] & 0xFF) << 16) |
                        ((nextPage[1] & 0xFF) << 8) |
                        (nextPage[2] & 0xFF);
            case 2:
                return ((page[offset] & 0xFF) << 24) |
                        ((page[offset + 1] & 0xFF) << 16) |
                        ((nextPage[0] & 0xFF) << 8) |
                        (nextPage[1] & 0xFF);
            case 3:
                return ((page[offset] & 0xFF) << 24) |
                        ((page[offset + 1] & 0xFF) << 16) |
                        ((page[offset + 2] & 0xFF) << 8) |
                        (nextPage[0] & 0xFF);
            default:
                throw new IllegalArgumentException("wrong boundary");
        }
    }

    public byte set(long index, byte value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final byte ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
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
        return new DeltaCursor(pages, pageSize, pageShift, pageMask);
    }

    private long allocate(long numberOfElements, BulkAdder into) {
        long intoIndex = allocIdx.getAndAdd(numberOfElements);
        grow(intoIndex + numberOfElements);
        into.grow(pages);
        into.init(intoIndex, numberOfElements);
        return intoIndex;
    }

    /**
     * Skip a region of {@code numberOfElements} that will not be allocated.
     */
    public final void skipAllocationRegion(long numberOfElements) {
        allocIdx.addAndGet(numberOfElements);
    }

    public final long release() {
        return super.release();
    }

    public DeltaCursor deltaCursor(DeltaCursor reuse, long offset) {
        return reuse.init(offset);
    }

    private static abstract class BaseCursor {

        private byte[][] pages;
        private int numPages;
        private final int pageSize;
        private final int pageShift;
        private final int pageMask;

        public byte[] array;
        public int offset;
        public int limit;

        private long from;
        private long to;
        private long size;
        private int fromPage;
        private int toPage;
        private int currentPage;

        BaseCursor(
                byte[][] pages,
                int pageSize,
                int pageShift,
                int pageMask) {
            this.pages = pages;
            this.pageSize = pageSize;
            this.pageShift = pageShift;
            this.pageMask = pageMask;
            this.numPages = pages.length;
        }

        void grow(byte[][] pages) {
            this.pages = pages;
            this.numPages = pages.length;
        }

        void init(long fromIndex, long length) {
            array = null;
            from = fromIndex;
            to = fromIndex + length;
            size = length;
            fromPage = PageUtil.pageIndex(fromIndex, pageShift);
            toPage = PageUtil.pageIndex(to - 1L, pageShift);
            currentPage = fromPage - 1;
        }

        void initAll(long fromIndex) {
            array = null;
            from = fromIndex;
            to = PageUtil.capacityFor(numPages, pageShift);
            size = to - fromIndex;
            fromPage = PageUtil.pageIndex(fromIndex, pageShift);
            toPage = numPages - 1;
            currentPage = fromPage - 1;
        }

        public final boolean next() {
            int current = ++currentPage;
            if (current >= pages.length) {
                System.out.println("current = " + current);
            }
            if (current == fromPage) {
                array = pages[current];
                offset = PageUtil.indexInPage(from, pageMask);
                int length = (int) Math.min((long) (pageSize - offset), size);
                limit = offset + length;
                return true;
            }
            if (current < toPage) {
                array = pages[current];
                offset = 0;
                limit = offset + pageSize;
                return true;
            }
            if (current == toPage) {
                array = pages[current];
                offset = 0;
                int length = PageUtil.indexInPage(to - 1L, pageMask) + 1;
                limit = offset + length;
                return true;
            }
            array = null;
            return false;
        }

        final void tryNext() {
            if (offset >= limit) {
                next();
            }
        }
    }

    public static final class LocalAllocator {
        private static final long PREFETCH_PAGES = 16L;

        private final ByteArray array;
        private final long prefetchSize;

        public final BulkAdder adder;

        private long top;
        private long limit;

        LocalAllocator(final ByteArray array) {
            this.array = array;
            this.adder = array.newBulkAdder();
            this.prefetchSize = (long) array.pageSize * PREFETCH_PAGES;
        }

        public long allocate(long size) {
            long address = top;
            if (address + size <= limit) {
                top += size;
                adder.tryNext();
                return address;
            }
            return majorAllocate(size);
        }

        private long majorAllocate(long size) {
            long allocate = Math.max(size, prefetchSize);
            long address = top = array.allocate(allocate, adder);
            limit = top + allocate;
            top += size;
            return address;
        }
    }

    public static final class BulkAdder extends BaseCursor {

        private BulkAdder(
                byte[][] pages,
                int pageSize,
                int pageShift,
                int pageMask) {
            super(pages, pageSize, pageShift, pageMask);
        }

        @Override
        public final void init(long fromIndex, long length) {
            super.init(fromIndex, length);
            next();
        }

        public void addUnsignedInt(int i) {
            if (limit - offset >= 4) {
                quickAddUnsignedInt(i);
            } else {
                slowAddUnsignedInt(i);
            }
        }

        public void addVLong(long i) {
            if (limit - offset >= 9) {
                quickAddVLong(i);
            } else {
                slowAddVLong(i);
            }
        }

        private void quickAddUnsignedInt(int i) {
            int offset = this.offset;
            byte[] array = this.array;
            array[offset++] = (byte) (i >>> 24);
            array[offset++] = (byte) (i >>> 16);
            array[offset++] = (byte) (i >>> 8);
            array[offset++] = (byte) (i);
            this.offset = offset;
        }

        private void slowAddUnsignedInt(int i) {
            byte[] array = this.array;
            int offset = this.offset;
            switch (limit - offset) {
                case 0:
                    if (!next()) {
                        break;
                    }
                    quickAddUnsignedInt(i);
                    break;
                case 1:
                    array[offset++] = (byte) (i >>> 24);
                    if (!next()) {
                        break;
                    }
                    array = this.array;
                    offset = this.offset;
                    array[offset++] = (byte) (i >>> 16);
                    array[offset++] = (byte) (i >>> 8);
                    array[offset++] = (byte) (i);
                    break;
                case 2:
                    array[offset++] = (byte) (i >>> 24);
                    array[offset++] = (byte) (i >>> 16);
                    if (!next()) {
                        break;
                    }
                    array = this.array;
                    offset = this.offset;
                    array[offset++] = (byte) (i >>> 8);
                    array[offset++] = (byte) (i);
                    break;
                case 3:
                    array[offset++] = (byte) (i >>> 24);
                    array[offset++] = (byte) (i >>> 16);
                    array[offset++] = (byte) (i >>> 8);
                    if (!next()) {
                        break;
                    }
                    array = this.array;
                    offset = this.offset;
                    array[offset++] = (byte) (i);
                    break;
                default:
                    throw new IllegalArgumentException("invalid boundaries");
            }
            this.offset = offset;
        }

        private void quickAddVLong(long i) {
            int offset = this.offset;
            byte[] array = this.array;

            while ((i & ~0x7FL) != 0L) {
                array[offset++] = (byte) ((i & 0x7FL) | 0x80L);
                i >>>= 7L;
            }
            array[offset++] = (byte) i;

            this.offset = offset;
        }

        private void slowAddVLong(long i) {
            int offset = this.offset;
            int limit = this.limit;
            byte[] array = this.array;

            while ((i & ~0x7FL) != 0L) {
                if (offset >= limit) {
                    if (!next()) {
                        return;
                    }
                    array = this.array;
                    offset = this.offset;
                    limit = this.limit;
                } else {
                    array[offset++] = (byte) ((i & 0x7FL) | 0x80L);
                    i >>>= 7L;
                }
            }

            this.offset = offset;
            if (offset >= limit) {
                if (!next()) {
                    return;
                }
            }

            this.array[this.offset++] = (byte) i;
        }
    }

    public static final class DeltaCursor extends BaseCursor {
        private int currentTarget;
        private int maxTargets;
        private long delta;

        private DeltaCursor(
                byte[][] pages,
                int pageSize,
                int pageShift,
                int pageMask) {
            super(pages, pageSize, pageShift, pageMask);
        }

        DeltaCursor init(long fromIndex) {
            super.initAll(fromIndex);
            next();

            currentTarget = 0;
            delta = 0L;
            if (limit - offset >= 4) {
                initLength(array, offset);
            } else {
                initLengthSlow();
            }

            return this;
        }

        public long getVLong() {
            if (currentTarget++ >= maxTargets) {
                return -1L;
            }
            return delta = getVLong0();
        }

        private long getVLong0() {
            if (limit - offset >= 9) {
                return getVLong(array, offset);
            }
            return slowGetVLong();
        }

        private void initLength(byte[] array, int offset) {
            this.maxTargets = ((array[offset++] & 0xFF) << 24) |
                    ((array[offset++] & 0xFF) << 16) |
                    ((array[offset++] & 0xFF) << 8) |
                    (array[offset++] & 0xFF);
            this.offset = offset;
        }

        private void initLengthSlow() {
            int offset = this.offset;
            int limit = this.limit;
            byte[] page1 = this.array;

            if (!next()) {
                return;
            }

            byte[] page2 = this.array;
            int offset2 = this.offset;

            switch (limit - offset) {
                case 0:
                    initLength(page2, offset2);
                    return;

                case 1:
                    this.maxTargets = ((page1[offset] & 0xFF) << 24) |
                            ((page2[offset2++] & 0xFF) << 16) |
                            ((page2[offset2++] & 0xFF) << 8) |
                            (page2[offset2++] & 0xFF);
                    break;

                case 2:
                    this.maxTargets = ((page1[offset++] & 0xFF) << 24) |
                            ((page1[offset] & 0xFF) << 16) |
                            ((page2[offset2++] & 0xFF) << 8) |
                            (page2[offset2++] & 0xFF);
                    break;

                case 3:
                    this.maxTargets = ((page1[offset++] & 0xFF) << 24) |
                            ((page1[offset++] & 0xFF) << 16) |
                            ((page1[offset] & 0xFF) << 8) |
                            (page2[offset2++] & 0xFF);
                    break;

                default:
                    throw new IllegalArgumentException("invalid boundary");
            }

            this.offset = offset2;
        }

        private long getVLong(byte[] page, int offset) {
            byte b = page[offset++];
            long i = (long) ((int) b & 0x7F);
            for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                b = page[offset++];
                i |= ((long) b & 0x7FL) << shift;
            }
            this.offset = offset;
            return i + delta;
        }

        private long slowGetVLong() {
            int diff = limit - offset;
            if (diff == 0) {
                if (!next()) {
                    return -1L;
                }
                return getVLong(this.array, this.offset);
            }

            byte[] array = this.array;
            int offset = this.offset;

            byte b = array[offset++];
            long i = (long) ((int) b & 0x7F);
            for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                if (--diff == 0) {
                    if (!next()) {
                        return -1L;
                    }
                    array = this.array;
                    offset = this.offset;
                }
                b = array[offset++];
                i |= ((long) b & 0x7FL) << shift;
            }
            this.offset = offset;
            return i + delta;
        }
    }
}
