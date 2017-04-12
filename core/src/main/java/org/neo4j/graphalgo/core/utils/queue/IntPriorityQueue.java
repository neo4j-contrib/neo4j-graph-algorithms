package org.neo4j.graphalgo.core.utils.queue;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Arrays;

/**
 * A PriorityQueue specialized for ints that maintains a partial ordering of
 * its elements such that the smallest value can always be found in constant time.
 * The definition of what <i>small</i> means is up to the implementing subclass.
 * <p>
 * Put()'s and pop()'s require log(size) time but the remove() cost implemented here is linear.
 * <p>
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class IntPriorityQueue implements PrimitiveIntIterable {
    public static final int DEFAULT_CAPACITY = 14;

    private int[] heap;
    private int size = 0;

    /**
     * Creates a new queue with an initial capacity of {@link #DEFAULT_CAPACITY}.
     *
     * @see #IntPriorityQueue(int)
     */
    protected IntPriorityQueue() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a new queue with the given capacity.
     * The queue dynamically grows to hold all elements.
     */
    protected IntPriorityQueue(final int initialCapacity) {
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
    }

    /**
     * Defines the ordering of the queue.
     * Returns true iff {@code a} is strictly less than {@code b}.
     * <p>
     * The default behavior assumes a min queue, where the smallest value is on top.
     * To implement a max queue, return {@code b < a}.
     * The resulting order is not stable.
     */
    protected abstract boolean lessThan(int a, int b);

    /**
     * Adds the cost for the given element.
     */
    protected abstract void addCost(int element, double cost);

    /**
     * Gets the cost for the given element.
     */
    protected abstract double cost(int element);

    /**
     * Optional callback for subclasses to get informed, when a value is removed
     * from the heap. This method is only called from {@link #pop()}, not from
     * {@link #clear()}.
     */
    protected void elementRemoved(int element) {
        // empty default behavior
    }

    /**
     * Adds an int associated with the given weight to a queue in log(size) time.
     * <p>
     * NOTE: The default implementation does nothing with the cost parameter.
     * It is up to the implementation how the cost parameter is used.
     *
     * @return the new 'top' element in the queue.
     */
    public final int add(int element, double cost) {
        addCost(element, cost);
        size++;
        ensureCapacityForInsert();
        heap[size] = element;
        upHeap(size);
        return heap[1];
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public final int top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap[1];
    }

    /**
     * @return the costs of least element in constant time.
     */
    public final double topCost() {
        return cost(top());
    }

    /**
     * Removes and returns the least element of the queue in log(size) time.
     *
     * @return the least element of the queue in log(size) time while removing it.
     */
    public final int pop() {
        if (size > 0) {
            int result = heap[1];    // save first value
            heap[1] = heap[size];    // move last to first
            size--;
            downHeap(1);           // adjust heap
            elementRemoved(result);
            return result;
        } else {
            return -1;
        }
    }

    /**
     * @return the number of elements currently stored in the queue.
     */
    public final int size() {
        return size;
    }

    /**
     * @return true iff there are currently no elements stored in the queue.
     */
    public final boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return true iff there is currently at least one element stored in the queue.
     */
    public final boolean nonEmpty() {
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
        heap[i] = node;              // install saved node
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
        heap[i] = node;              // install saved node
    }

    private void ensureCapacityForInsert() {
        if (size >= heap.length) {
            heap = Arrays.copyOf(
                    heap,
                    ArrayUtil.oversize(size + 1, Integer.BYTES));
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

            /**
             * @throws ArrayIndexOutOfBoundsException when the iterator is exhausted.
             */
            @Override
            public int next() {
                return heap[i++];
            }
        };
    }
}
