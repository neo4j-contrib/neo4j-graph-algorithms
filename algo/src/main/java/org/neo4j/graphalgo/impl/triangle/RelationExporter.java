package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

import java.util.function.BiFunction;

/**
 * @author mknblch
 */
public class RelationExporter extends StatementApi {

    private final Integer propertyId;

    public RelationExporter(GraphDatabaseAPI api, String property) {
        super(api);
        propertyId = applyInTransaction(s -> s.tokenWrite().propertyKeyGetOrCreateForName(property));
    }

    public void export(BiFunction<Long, Long, Value> getter) {

        acceptInTransaction(statement -> {
            for (Relationship relationship : api.getAllRelationships()) {
                final Value value = getter.apply(relationship.getStartNodeId(), relationship.getEndNodeId());
                if (null == value) {
                    continue;
                }
                statement.dataWrite().relationshipSetProperty(relationship.getId(), propertyId, value);
            }
        });
    }

}
