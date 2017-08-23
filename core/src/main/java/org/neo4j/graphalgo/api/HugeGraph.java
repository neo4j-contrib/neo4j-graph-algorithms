package org.neo4j.graphalgo.api;


import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

/**
 * Composition of often used source interfaces
 *
 * @author mknblch
 */
public interface HugeGraph extends HugeIdMapping, HugeDegrees, HugeNodeIterator, HugeBatchNodeIterable, HugeRelationshipIterator, HugeRelationshipWeights, Graph {

    /**
     * release resources which are not part of the result or IdMapping
     */
    default void release() {

    }

    @Override
    default Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return hugeBatchIterables(batchSize)
                .stream()
                .map(l -> (PrimitiveIntIterable) () -> new LongToIntIterator(l.iterator()))
                .collect(Collectors.toList());
    }

    @Override
    default int degree(int nodeId, Direction direction) {
        return degree((long) nodeId, direction);
    }

    @Override
    default int toMappedNodeId(long nodeId) {
        return (int) toHugeMappedNodeId(nodeId);
    }

    @Override
    default long toOriginalNodeId(int nodeId) {
        return toOriginalNodeId((long) nodeId);
    }

    @Override
    default int nodeCount() {
        return (int) hugeNodeCount();
    }

    @Override
    default void forEachNode(IntPredicate consumer) {
        forEachNode((LongPredicate) l -> consumer.test((int) l));
    }

    @Override
    default PrimitiveIntIterator nodeIterator() {
        return new LongToIntIterator(hugeNodeIterator());
    }

    @Override
    default void forEachRelationship(
            int nodeId,
            Direction direction,
            RelationshipConsumer consumer) {
        forEachRelationship((long) nodeId, direction,
                (s, t) -> consumer.accept(
                        (int) s,
                        (int) t,
                        RawValues.combineIntInt(
                                direction,
                                (int) s,
                                (int) t)));
    }

    @Override
    default void forEachRelationship(
            int nodeId,
            Direction direction,
            WeightedRelationshipConsumer consumer) {
        forEachRelationship((long) nodeId, direction,
                (s, t) -> {
                    double weight = direction == Direction.OUTGOING
                            ? HugeGraph.this.weightOf(s, t)
                            : HugeGraph.this.weightOf(t, s);
                    return consumer.accept(
                            (int) s,
                            (int) t,
                            RawValues.combineIntInt(
                                    direction,
                                    (int) s,
                                    (int) t
                            ),
                            weight);
                });
    }

    @Override
    default void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        forEachOutgoing(
                (long) nodeId,
                (s, t) -> consumer.accept((int) s,
                        (int) t,
                        RawValues.combineIntInt((int) s, (int) t)));
    }

    @Override
    default void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        forEachIncoming(
                (long) nodeId,
                (s, t) -> consumer.accept((int) s,
                        (int) t,
                        RawValues.combineIntInt((int) t, (int) s)));
    }

    final class LongToIntIterator implements PrimitiveIntIterator {
        private final PrimitiveLongIterator iter;

        LongToIntIterator(final PrimitiveLongIterator iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public int next() {
            return (int) iter.next();
        }
    }
}
