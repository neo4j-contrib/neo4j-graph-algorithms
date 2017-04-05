package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Iterator;

/**
 *
 * [node][degree, targetNode, f0, ... fn, targetNode, f0, ... fn, ...]
 *
 * @author mknblch
 */
public class DirectedGraph {

    private final IntIntMap map;
    private final int frameSize;
    private final int headSize;

    private int[][] frames;
    private int offset = 0;

    public DirectedGraph(int capacity, int frameSize) {
        this.frames = new int[capacity][];
        this.map = new IntIntScatterMap(capacity);
        this.frameSize = frameSize;
        this.headSize = frameSize + 2; // degree + targetNode + frameSize
    }

    public int degree(int nodeId) {
        return frames[map.get(nodeId)][0];
    }

    public int nodeCount() {
        return offset;
    }

    public void connect(int sourceNodeId, int targetNodeId) {
        final int p = addOrGetNode(sourceNodeId);
        final int q = addOrGetNode(targetNodeId);
        growNode(p);
        final int offset = frames[p][0]++;
        frames[p][offset] = q; // first spot targetNode
    }

    public void connect(int sourceNodeId, int targetNodeId, int... data) {
        final int p = addOrGetNode(sourceNodeId);
        final int q = addOrGetNode(targetNodeId);
        growNode(p);
        final int offset = frames[p][0]++ * frameSize + 1;
        frames[p][offset + 1] = q; // first spot targetNode
        System.arraycopy(data, 0, frames[p], offset, data.length);
    }

    public PrimitiveIntIterator nodeIterator() {
        return new NodeIterator(map);
    }

    public Iterator<Frame> frameIterator(int sourceNodeId) {
        return new FrameIterator(frames[map.get(sourceNodeId)], frameSize);
    }

    private void growNode(int p) {
        int[] frame = frames[p];
        int newSize = frame.length + frameSize + 1;
        if (frame.length < newSize) {
            int[] temp = new int[ArrayUtil.oversize(newSize, Integer.BYTES)];
            System.arraycopy(frame, 0, temp, 0, frame.length);
            frames[p] = temp;
        }
    }

    private int addOrGetNode(int nodeId) {
        if (map.containsKey(nodeId)) {
            return map.get(nodeId);
        }
        map.put(nodeId, offset);
        final int c = offset++;
        if (frames.length < offset) {
            int[][] temp = new int[ArrayUtil.oversize(offset, Integer.BYTES)][];
            System.arraycopy(frames, 0, temp, 0, c);
            frames = temp;
        }
        frames[c] = new int[headSize];
        return c;
    }

    public static class Frame {

        private final int[] frames;
        private int offset;

        public Frame(int[] frames) {
            this.frames = frames;
        }

        public int targetNode() {
            return frames[offset];
        }

        public int value(int key) {
            return frames[offset + key + 1];
        }
    }

    private static class FrameIterator implements Iterator<Frame> {

        private final Frame frame;
        private final int frameSize;
        private final int size;
        private int offset = 0;

        private FrameIterator(int[] frames, int frameSize) {
            this.size = frames.length;
            this.frame = new Frame(frames);
            this.frameSize = frameSize;
        }

        @Override
        public boolean hasNext() {
            return offset < size;
        }

        @Override
        public Frame next() {
            frame.offset += frameSize;
            return frame;
        }
    }


    private static class NodeIterator implements PrimitiveIntIterator {

        private final IntIntMap map;
        private final int size;

        private int offset = 0;

        private NodeIterator(IntIntMap map) {
            this.map = map;
            size = map.size();
        }

        @Override
        public boolean hasNext() {
            return offset < size;
        }

        @Override
        public int next() {
            return map.get(offset++);
        }
    }

}
