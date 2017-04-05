package org.neo4j.graphalgo.core.utils;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Iterator;

/**
 * dynamically growing list of int-tuples
 *
 * TODO: specialized 2Tuple or 3Tuple ?
 *
 * @author mknblch
 */
public class IntTupleList implements Iterable<int[]> {

    public final int elements;
    private int[] data;
    private final int startIndex;
    private int offset;
    private int index;

    /**
     * @param elements number of elements in the tuple
     * @param initialSize number of tuples to save
     */
    public IntTupleList(int elements, int initialSize) {
        this.elements = elements;
        this.data = new int[initialSize * elements];
        this.offset = 0;
        this.startIndex = 0;
    }

    private IntTupleList(int[] data, int elements, int startIndex, int endIndex) {
        this.data = data;
        this.elements = elements;
        this.startIndex = startIndex;
        this.offset = endIndex;
    }

    public int add(int... tuple) {
        grow(offset+elements);
        System.arraycopy(tuple, 0, data, offset, tuple.length);
        int index = (offset - startIndex) / elements;
        offset += elements;
        return index;
    }

    public int size() {
        return (offset - startIndex) / elements;
    }

    public void set(int index, int key, int value) {
        data[index * elements + key] = value;
    }

    public int get(int index, int key) {
        return data[index * elements + key];
    }

    public IntTupleList slice(int from, int to) {
        return new IntTupleList(data, elements, startIndex + from * elements, startIndex + to * elements);
    }

    @Override
    public Iterator<int[]> iterator() {
        return new TupleIterator(data, elements, startIndex, offset);
    }

    public PrimitiveIntIterator elementIterator(int element) {
        return new ElementIterator(data, elements, element, startIndex, offset);
    }

    private void grow(int newElementCount) {
        if (newElementCount <= data.length) {
            return;
        }
        int end = newElementCount * elements;
        data = ArrayUtil.grow(data, end);
    }

    private static class TupleIterator implements Iterator<int[]> {

        private final int[] data;
        private final int[] tuple;
        private final int end;
        private final int elements;
        private int offset;

        private TupleIterator(int[] data, int elements, int start, int size) {
            this.data = data;
            this.elements = elements;
            this.tuple = new int[elements];
            this.offset = start;
            this.end = size;
        }

        @Override
        public boolean hasNext() {
            return offset < end;
        }

        @Override
        public int[] next() {
            System.arraycopy(data, offset, tuple, 0, elements);
            offset += elements;
            return tuple;
        }
    }

    private static class ElementIterator implements PrimitiveIntIterator {

        private final int[] data;
        private final int[] tuple;
        private final int end;
        private final int elements;
        private final int element;
        private int offset;

        private ElementIterator(int[] data, int elements, int key, int start, int size) {
            this.data = data;
            this.elements = elements;
            this.tuple = new int[elements];
            this.element = key;
            this.offset = start;
            this.end = size;
        }

        @Override
        public boolean hasNext() {
            return offset < end;
        }

        @Override
        public int next() {
            return data[offset + element];
        }
    }
}
