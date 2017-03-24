package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.NoSuchElementException;


/**
 * A PriorityQueue specialized for ints that maintains a partial ordering of
 * its elements such that the smallest value can always be found in constant time.
 * Put()'s and pop()'s require log(size) time but the remove() cost implemented here is linear.
 * <p>
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class IntMinPriorityQueue implements PrimitiveIntIterable {
    private static final int DEFAULT_CAPACITY = 14;
    private int size = 0;
    private int[] heap;
    private final IntDoubleScatterMap costs;

    public IntMinPriorityQueue() {
        this(DEFAULT_CAPACITY);
    }

    public IntMinPriorityQueue(final int initialCapacity) {
        final int heapSize;
        if (0 == initialCapacity) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based.  heap[0] is unused.
            heapSize = initialCapacity + 1;
        }
        this.heap = new int[ArrayUtil.oversize(heapSize, Integer.BYTES)];
        this.costs = new IntDoubleScatterMap(heapSize);
    }

    private boolean lessThan(int a, int b) {
        return costs.get(a) < costs.get(b);
    }

    /**
     * Adds an int associated with the given weight to a queue in log(size) time.
     *
     * @return the new 'top' element in the queue.
     */
    public int add(int element, double cost) {
        size++;
        ensureCapacityForInsert();
        heap[size] = element;
        costs.addTo(element, cost);
        upHeap(size);
        return heap[1];
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public int top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap[1];
    }

    /**
     * Removes and returns the least element of the queue in log(size) time.
     *
     * @return the least element of the queue in log(size) time while removing it.
     */
    public int pop() {
        if (size > 0) {
            int result = heap[1];       // save first value
            heap[1] = heap[size];     // move last to first
            size--;
            downHeap(1);              // adjust heap
            costs.remove(result);
            return result;
        } else {
            return -1;
        }
    }

    /**
     * @return the number of elements currently stored in the queue.
     */
    public int size() {
        return size;
    }

    /**
     * @return true iff there are currently no elements stored in the queue.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return true iff there is currently at least one element stored in the queue.
     */
    public boolean nonEmpty() {
        return size != 0;
    }

    /**
     * Removes all entries from the queue.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Removes all entries from the queue, releases all buffers.
     * The queue can no longer be used afterwards.
     */
    public void release() {
        size = 0;
        heap = null;

        costs.keys = new int[0];
        costs.clear();
        costs.keys = null;
        costs.values = null;
    }

    private boolean upHeap(int origPos) {
        int i = origPos;
        int node = heap[i];          // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j])) {
            heap[i] = heap[j];       // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap[i] = node;            // install saved node
        return i != origPos;
    }

    private void downHeap(int i) {
        int node = heap[i];          // save top node
        int j = i << 1;              // find smaller child
        int k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j])) {
            j = k;
        }
        while (j <= size && lessThan(heap[j], node)) {
            heap[i] = heap[j];       // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j])) {
                j = k;
            }
        }
        heap[i] = node;            // install saved node
    }

    private void ensureCapacityForInsert() {
        if (size >= heap.length) {
            int[] newHeap = new int[ArrayUtil.oversize(
                    size + 1,
                    Integer.BYTES)];
            System.arraycopy(heap, 0, newHeap, 0, heap.length);
            heap = newHeap;
        }
    }

    @Override
    public PrimitiveIntIterator iterator() {
        return new PrimitiveIntIterator() {

            int i = 1;

            @Override
            public boolean hasNext() {
                return i <= size;
            }

            @Override
            public int next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return heap[i++];
            }
        };
    }

}
