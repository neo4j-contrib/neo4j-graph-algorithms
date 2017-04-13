package org.neo4j.graphalgo.core.sources;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
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
                final Cursor<PropertyItem> property = c.property(propertyId);
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

    public static ARIImporter importer(GraphDatabaseAPI api) {
        return new ARIImporter(api);
    }


    public static class ARIImporter extends Importer<SingleRunWeightedAllRelationIterator, ARIImporter> {

        private IdMapping idMapping;

        public ARIImporter(GraphDatabaseAPI api) {
            super(api);
        }

        public ARIImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = Objects.requireNonNull(idMapping);
            return this;
        }

        @Override
        protected ARIImporter me() {
            return this;
        }

        @Override
        protected SingleRunWeightedAllRelationIterator buildT() {
            return new SingleRunWeightedAllRelationIterator(api,
                    idMapping,
                    relationId,
                    propertyId,
                    propertyDefaultValue);
        }
    }
}
