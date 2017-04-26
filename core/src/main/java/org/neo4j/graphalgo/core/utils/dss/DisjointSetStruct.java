package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public final class DisjointSetStruct {

    private final int[] parent;
    private final int[] depth;
    private final int capacity;

    /**
     * Initialize the struct with the given capacity.
     * Note: the struct must be {@link DisjointSetStruct#reset()} prior use!
     * @param capacity the capacity (maximum node id)
     */
    public DisjointSetStruct(int capacity) {
        parent = new int[capacity];
        depth = new int[capacity];
        this.capacity = capacity;
    }

    /**
     * reset the container
     */
    public void reset() {
        for (int i = 0; i < capacity; i++) {
            parent[i] = i;
        }
    }

    /**
     * iterate each node and find its setId
     * @param consumer the consumer
     */
    public void forEach(Consumer consumer) {
        for (int i = parent.length - 1; i >= 0; i--) {
            if (!consumer.consume(i, findPC(i))) {
                break;
            }
        }
    }

    /**
     * @return return a Iterator for each nodeIt-setId combination
     */
    public Iterator<Cursor> iterator() {
        return new NodeSetIterator(this);
    }

    /**
     * @param start startNodeId
     * @param length number of nodes to process
     * @return return an Iterator over each nodeIt-setId combination within its bounds
     */
    public Iterator<Cursor> iterator(int start, int length) {
        return new ConcurrentNodeSetIterator(this, start, length);
    }

    public Stream<Result> resultStream(IdMapping idMapping) {

        return IntStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }

    /**
     * element count
     * @return the element count
     */
    public int count() {
        return parent.length;
    }


    /**
     * find setId of element p.
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int find(int p) {
        return findPC(p);
    }

    /**
     * find setId of element p without balancing optimization.
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int findNoOpt(int p) {
        while (p != parent[p]) {
            p = parent[parent[p]];
        }
        return p;
    }

    /**
     * find setId of element p.
     *
     * Non-recursive implementation using path-halving optimization and tree balancing
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int findPH(int p) {
        while (p != parent[p]) {
            p = parent[p] = parent[parent[p]];
        }
        return p;
    }

    /**
     * find setId of element p.
     *
     * find-impl using a recursive path compression logic
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int findPC(int p) {
        if (parent[p] == p) return p;
        // path compression optimization
        parent[p] = find(parent[p]); // balance tree while traversing
        return parent[p];
    }

    /**
     * check if p and q belong to the same set
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }


    /**
     * join set of p (Sp) with set of q (Sq) so that {@link DisjointSetStruct#connected(int, int)}
     * for any pair of (Spi, Sqj) evaluates to true
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    public void union(int p, int q) {
        final int pSet = find(p);
        final int qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        int dq = depth[qSet];
        int dp = depth[pSet];
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent[pSet] = qSet;
        } else if (dp > dq) {
            parent[qSet] = pSet;
        } else {
            parent[qSet] = pSet;
            depth[pSet] += depth[qSet] + 1;
        }
    }

    public int getSetCount() {
        final IntSet set = new IntScatterSet();
        forEach((nodeId, setId) -> {
            set.add(setId);
            return true;
        });
        return set.size();
    }

    /**
     * evaluate the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    public IntIntMap getSetSize() {
        final IntIntScatterMap map = new IntIntScatterMap();
        for (int i = parent.length - 1; i >= 0; i--) {
            map.addTo(find(i), 1);
        }
        return map;
    }

    /**
     * Consumer interface for c
     */
    @FunctionalInterface
    public interface Consumer {
        /**
         * @param nodeId the mapped node id
         * @param setId the set id where the node belongs to
         * @return true to continue the iteration, false to stop
         */
        boolean consume(int nodeId, int setId);
    }

    public static class Cursor {
        /**
         * the mapped node id
         */
        int nodeId;
        /**
         * the set id of the node
         */
        int setId;
    }

    /**
     * Iterator only usable for single threaded evaluation.
     * Does tree-balancing during iteration.
     */
    private static class NodeSetIterator implements Iterator<Cursor> {

        private final Cursor cursor = new Cursor();
        private final DisjointSetStruct struct;
        private final int length;

        private int offset = IdMapping.START_NODE_ID;

        private NodeSetIterator(DisjointSetStruct struct) {
            this.struct = struct;
            this.length = struct.count();
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public Cursor next() {
            cursor.nodeId = offset;
            cursor.setId = struct.find(offset);
            return cursor;
        }
    }

    /**
     * Iterator usable in multithreaded environment
     */
    private static class ConcurrentNodeSetIterator implements Iterator<Cursor> {

        private final Cursor cursor = new Cursor();
        private final DisjointSetStruct struct;
        private final int length;

        private int offset = 0;

        private ConcurrentNodeSetIterator(DisjointSetStruct struct, int startOffset, int length) {
            this.struct = struct;
            this.length = length + offset > struct.count()
                    ? struct.count() - offset
                    : length;
            this.offset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public Cursor next() {
            cursor.nodeId = offset;
            cursor.setId = struct.findNoOpt(offset);
            return cursor;
        }
    }

    public static class DSSExporter extends Exporter<DisjointSetStruct> {

        private final IdMapping idMapping;
        private int propertyId;

        public DSSExporter(GraphDatabaseAPI api, IdMapping idMapping, String targetProperty) {
            super(api);
            this.idMapping = idMapping;
            propertyId = getOrCreatePropertyId(targetProperty);
        }

        @Override
        public void write(DisjointSetStruct struct) {
            writeInTransaction(writeOp -> {
                struct.forEach((nodeId, setId) -> {
                    try {
                        writeOp.nodeSetProperty(
                                idMapping.toOriginalNodeId(nodeId),
                                DefinedProperty.numberProperty(propertyId, setId));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                });
            });
        }
    }

    public static class Result {

        /**
         * the mapped node id
         */
        public final Long nodeId;

        /**
         * set id
         */
        public final Long setId;

        public Result(long nodeId, int setId) {
            this.nodeId = nodeId;
            this.setId = (long) setId;
        }
    }
}
