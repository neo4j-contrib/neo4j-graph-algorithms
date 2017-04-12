package org.neo4j.graphalgo.core.sources;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.PropertyItem;

import java.util.Iterator;
import java.util.Objects;

/**
 * Unbuffered RelationshipIterator which is directly
 * @author mknblch
 */
public class SingleRunWeightedAllRelationIterator implements WeightedAllRelationshipIterator {

    private final GraphDatabaseAPI api;
    private final IdMapping idMapping;
    private final int[] relationId; // TODO
    private final int propertyId;
    private final double propertyDefaultValue;

    private SingleRunWeightedAllRelationIterator(GraphDatabaseAPI api, IdMapping idMapping, int[] relationId, int propertyId, double propertyDefaultValue) {
        this.api = api;
        this.idMapping = idMapping;
        this.relationId = relationId;
        this.propertyId = propertyId;
        this.propertyDefaultValue = propertyDefaultValue;
    }

    @Override
    public void forEachRelationship(WeightedRelationshipConsumer consumer) {
        try (Transaction transaction = api.beginTx()) {
            final ReadOperations readOperations = api.getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()
                    .readOperations();
            readOperations.relationshipCursorGetAll().forAll(c -> {
                double weight = propertyDefaultValue;
                final Cursor<PropertyItem> property = c.property(propertyId); // TODO bug?
                if (property.next()) {
                    weight = RawValues.extractValue(property.get().value(), propertyDefaultValue);
                }
                long startNode = c.startNode();
                consumer.accept(
                        idMapping.toMappedNodeId(startNode),
                        idMapping.toMappedNodeId(c.otherNode(startNode)),
                        -1L,
                        weight);
            });
            transaction.success();
        }
    }

    @Override
    public Iterator<WeightedRelationshipCursor> allWeightedRelationshipIterator() {
        throw new UnsupportedOperationException();
    }

    public static Builder builder(GraphDatabaseAPI api) {
        return new Builder(api);
    }

    public static class Builder {

        private final GraphDatabaseAPI api;

        private IdMapping idMapping;
        private String relationship;
        private String property;
        private double propertyDefaultValue;

        public Builder(GraphDatabaseAPI api) {
            this.api = api;
        }

        public Builder withIdMapping(IdMapping idMapping) {
            this.idMapping = Objects.requireNonNull(idMapping);
            return this;
        }

        /**
         * TODO not implemented yet
         *
         * @param relationship
         * @return
         */
        public Builder withRelationship(String relationship) {
            this.relationship = Objects.requireNonNull(relationship);
            return this;
        }

        public Builder withProperty(String property, double propertyDefaultValue) {
            this.property = Objects.requireNonNull(property);
            this.propertyDefaultValue = propertyDefaultValue;
            return this;
        }

        public Builder withPropertyDefaultValue(double propertyDefaultValue) {
            this.propertyDefaultValue = propertyDefaultValue;
            return this;
        }

        public SingleRunWeightedAllRelationIterator build() {

            try (Transaction tx = api.beginTx();
                 Statement statement = api.getDependencyResolver()
                         .resolveDependency(ThreadToStatementContextBridge.class)
                         .get()) {
                final ReadOperations readOp = statement.readOperations();
                final int propertyId = readOp.propertyKeyGetForName(property);
                tx.success();
                return new SingleRunWeightedAllRelationIterator(api, idMapping, null, propertyId, propertyDefaultValue);
            }
        }
    }
}
