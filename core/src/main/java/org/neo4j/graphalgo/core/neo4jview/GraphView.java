package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.*;

/**
 * A Graph implemented as View on Neo4j Kernel API
 *
 * @author mknobloch
 */
public class GraphView implements Graph {

    private final ThreadToStatementContextBridge contextBridge;
    private final GraphDatabaseAPI db;

    private final double propertyDefaultWeight;
    private int relationTypeId;
    private int nodeCount;
    private int propertyKey;
    private int labelId;
    private final IdMapping idMapping;

    public GraphView(GraphDatabaseAPI db, String label, String relation, String propertyName, double propertyDefaultWeight) {
        this.db = db;
        contextBridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        this.propertyDefaultWeight = propertyDefaultWeight;

        withinTransaction(read -> {
            labelId = read.labelGetForName(label);
            nodeCount = Math.toIntExact(read.countsForNode(labelId));
            relationTypeId = read.relationshipTypeGetForName(relation);
            propertyKey = read.propertyKeyGetForName(propertyName);
        });
        idMapping = createIdMapping();
    }

    private IdMapping createIdMapping() {
        if (labelId == StatementConstants.NO_SUCH_LABEL) return new DirectIdMapping(nodeCount);
        IdMap idMap = new IdMap(nodeCount);
        // TODO parallelize?
        withinTransaction(read -> {
            PrimitiveLongIterator it = read.nodesGetForLabel(labelId);
            while (it.hasNext()) {
                idMap.add(it.next());
            }
        });
        idMap.buildMappedIds();
        return idMap;
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        final WeightedRelationshipConsumer asWeighted =
                (sourceNodeId, targetNodeId, relationId, weight) ->
                        consumer.accept(sourceNodeId, targetNodeId, relationId);
        forAllRelationships(nodeId, direction, false, asWeighted);
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        forAllRelationships(nodeId, direction, true, consumer);
    }

    private void forAllRelationships(
            int nodeId,
            Direction direction,
            boolean readWeights,
            WeightedRelationshipConsumer action) {
        final long originalNodeId = toOriginalNodeId(nodeId);
        try {
            withinTransaction(read -> {
                final double defaultWeight = this.propertyDefaultWeight;
                RelationshipVisitor<EntityNotFoundException> visitor = (relationshipId, typeId, startNodeId, endNodeId) -> {
                    long otherNodeId = startNodeId == originalNodeId ? endNodeId : startNodeId;
                    if (idMapping.contains(otherNodeId)) {
                        double weight = defaultWeight;
                        if (readWeights && read.relationshipHasProperty(relationshipId, propertyKey)) {
                            Object value = read.relationshipGetProperty(
                                    relationshipId,
                                    propertyKey);
                            weight = RawValues.extractValue(
                                    value,
                                    defaultWeight);
                        }
                        final int otherId = toMappedNodeId(otherNodeId);

                        long relId = RawValues.combineIntInt((int) startNodeId, (int) endNodeId);
                        action.accept(nodeId, otherId, relId, weight);
                    }
                };

                final RelationshipIterator rels;
                if (relationTypeId == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                    rels = read.nodeGetRelationships(originalNodeId, direction);
                } else {
                    rels = read.nodeGetRelationships(originalNodeId, direction, relationTypeId);
                }
                while (rels.hasNext()) {
                    final long relId = rels.next();
                    rels.relationshipVisit(relId, visitor);
                }
            });
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }

    @Override
    public int nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        withinTransaction(read -> {
            PrimitiveLongIterator nodes = labelId == StatementConstants.NO_SUCH_LABEL
                    ? read.nodesGetAll()
                    : read.nodesGetForLabel(labelId);
            while (nodes.hasNext()) {
                final long nodeId = nodes.next();
                if (!consumer.test(toMappedNodeId(nodeId))) {
                    break;
                }
            }
        });
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return withinTransactionTyped(read -> {
            if (labelId == StatementConstants.NO_SUCH_LABEL) {
                return new NodeIterator(this, read.nodesGetAll());
            }
            return new NodeIterator(this, read.nodesGetForLabel(labelId));
        });
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(final int batchSize) {
        int nodeCount = this.nodeCount;
        int numberOfBatches = (int) Math.ceil(nodeCount / (double) batchSize);
        if (numberOfBatches == 1) {
            return Collections.singleton(this::nodeIterator);
        }
        PrimitiveIntIterable[] iterators = new PrimitiveIntIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> () -> withinTransactionTyped(read -> {
            PrimitiveLongIterator neoIds;
            if (labelId == StatementConstants.NO_SUCH_LABEL) {
                neoIds = read.nodesGetAll();
            } else {
                neoIds = read.nodesGetForLabel(labelId);
            }
            return new SizedNodeIterator(this, neoIds, i * batchSize, batchSize);
        }));
        return Arrays.asList(iterators);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        return withinTransactionInt(read -> {
            try {
                return read.nodeGetDegree(toOriginalNodeId(nodeId), direction, relationTypeId);
            } catch (EntityNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    private int withinTransactionInt(ToIntFunction<ReadOperations> block) {
        try (final Transaction tx = db.beginTx();
             Statement statement = contextBridge.get()) {
            final int result = block.applyAsInt(statement.readOperations());
            tx.success();
            return result;
        }
    }

    private <T> T withinTransactionTyped(Function<ReadOperations, T> block) {
        try (final Transaction tx = db.beginTx();
             Statement statement = contextBridge.get()) {
            final T result = block.apply(statement.readOperations());
            tx.success();
            return result;
        }
    }

    private <E extends Exception> void withinTransaction(CheckedConsumer<ReadOperations, E> block)
    throws E {
        try (final Transaction tx = db.beginTx();
             Statement statement = contextBridge.get()) {
            block.accept(statement.readOperations());
            tx.success();
        }
    }

    private interface CheckedConsumer<T, E extends Exception> {
        void accept(T t) throws E;
    }


    private static class NodeIterator implements PrimitiveIntIterator {

        private final Graph graph;

        private final PrimitiveLongIterator iterator;

        private NodeIterator(Graph graph, PrimitiveLongIterator iterator) {
            this.graph = graph;
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public int next() {
            return graph.toMappedNodeId(iterator.next());
        }
    }

    private static class SizedNodeIterator implements PrimitiveIntIterator {

        private final Graph graph;

        private final PrimitiveLongIterator iterator;
        private int remaining;

        private SizedNodeIterator(
                Graph graph,
                PrimitiveLongIterator iterator,
                int start,
                int length) {
            while (iterator.hasNext() && start-- > 0) {
                iterator.next();
            }
            this.graph = graph;
            this.iterator = iterator;
            this.remaining = length;
        }

        @Override
        public boolean hasNext() {
            return remaining > 0 && iterator.hasNext();
        }

        @Override
        public int next() {
            remaining--;
            return graph.toMappedNodeId(iterator.next());
        }
    }
}
