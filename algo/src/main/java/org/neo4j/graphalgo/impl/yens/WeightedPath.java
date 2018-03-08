package org.neo4j.graphalgo.impl.yens;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class WeightedPath {

    public interface EdgeConsumer {

        void accept(int sourceNode, int targetNode);
    }

    private int[] nodes;
    private int offset = 0;

    public WeightedPath() {
        nodes = new int[10];
    }

    public void append(int nodeId) {
        nodes = ArrayUtil.grow(nodes, offset + 1);
        nodes[offset++] = nodeId;
    }

    public int node(int index) {
        return nodes[index];
    }

    public int size() {
        return offset;
    }

    public void forEach(IntPredicate consumer) {
        for (int i = 0; i < offset; i++) {
            if (!consumer.test(nodes[i])) {
                return;
            }
        }
    }

    public void forEachEdge(EdgeConsumer consumer) {
        for (int i = 0; i < offset - 1; i++) {
            consumer.accept(nodes[i], nodes[i + 1]);
        }
    }

    public void forEachDo(IntConsumer consumer) {
        for (int i = 0; i < offset; i++) {
            consumer.accept(nodes[i]);
        }
    }

    public WeightedPath pathTo(int end) {
        if (end > size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        final WeightedPath path = new WeightedPath();
        System.arraycopy(nodes, 0, path.nodes, 0, end);
        path.offset = end;
        return path;
    }

    public long edge(int i) {
        return RawValues.combineIntInt(nodes[i], nodes[i + 1]);
    }

    public void clear() {
        offset = 0;
    }

    public void revert() {
        for (int i = 0; i < offset / 2; i++) {
            int temp = nodes[i];
            nodes[i] = nodes[offset - 1 - i];
            nodes[offset - 1 - i] = temp;
        }
    }

    public String toString() {
        final StringBuilder buffer = new StringBuilder(size());
        forEachDo(n -> {
            if (buffer.length() != 0) {
                buffer.append(",");
            }
            buffer.append(n);
        });
        return buffer.toString();
    }

    public boolean equalsTo(WeightedPath other, int length) {
        if (size() < length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (nodes[i] != other.nodes[i]) {
                return false;
            }
        }
        return true;
    }

    public int[] toArray() {
        return Arrays.copyOf(nodes, offset);
    }

    public static WeightedPath concat(WeightedPath head, WeightedPath tail) {
        final int off = head.size() + tail.size();
        final WeightedPath path = new WeightedPath();
        System.arraycopy(head.nodes, 0, path.nodes, 0, head.size());
        System.arraycopy(tail.nodes, 0, path.nodes, head.size(), tail.size());
        path.offset = off;
        return path;
    }

    public static double getCosts(HeavyGraph graph, WeightedPath path) {
        double[] costs = {0.0};
        path.forEachEdge((s, t) -> {
            costs[0] += graph.weightOf(s, t);
        });
        return costs[0];
    }
}
