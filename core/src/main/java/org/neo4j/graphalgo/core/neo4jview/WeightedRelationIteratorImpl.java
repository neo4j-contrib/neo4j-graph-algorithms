package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightedRelationshipCursor;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.io.Closeable;
import java.util.Iterator;

class WeightedRelationIteratorImpl implements Iterator<WeightedRelationshipCursor>, Closeable {

    private final Graph graph;
    private final Statement statement;
    private final Transaction tx;
    private final int propertyKey;
    private final WeightedRelationshipCursor cursor;
    private final long originalNodeId;
    private final RelationshipIterator relationships;
    private final ReadOperations read;
    private final IdCombiner relId;
    private double defaultWeight;

    WeightedRelationIteratorImpl(Graph graph, GraphDatabaseAPI api, int sourceNodeId, Direction direction, int relationId, int propertyId, double propertyDefaultWeight) throws EntityNotFoundException {
        this.graph = graph;
        this.propertyKey = propertyId;
        this.defaultWeight = propertyDefaultWeight;
        this.tx = api.beginTx();
        this.statement = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        this.read = statement.readOperations();
        this.originalNodeId = graph.toOriginalNodeId(sourceNodeId);
        if (relationId == ReadOperations.ANY_RELATIONSHIP_TYPE) {
            relationships = read.nodeGetRelationships(originalNodeId, direction);
        } else {
            relationships = read.nodeGetRelationships(originalNodeId, direction, relationId);
        }
        this.cursor = new WeightedRelationshipCursor();
        cursor.sourceNodeId = sourceNodeId;
        relId = RawValues.combiner(direction);
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext = relationships.hasNext();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public WeightedRelationshipCursor next() {

        final long relationId = relationships.next();
        final Cursor<RelationshipItem> relCursor = read.relationshipCursor(relationId);
        relCursor.next();
        final RelationshipItem item = relCursor.get();
        cursor.targetNodeId = graph.toMappedNodeId(item.otherNode(originalNodeId));
        cursor.relationshipId = relId.apply(cursor);
        final Cursor<PropertyItem> propertyCursor = item.property(propertyKey);
        if (propertyCursor.next()) {
            cursor.weight = ((Number) propertyCursor.get().value()).doubleValue();
        } else {
            cursor.weight = defaultWeight;
        }
        return cursor;
    }

    @Override
    public void close() {
        statement.close();
        tx.success();
        tx.close();
    }
}
