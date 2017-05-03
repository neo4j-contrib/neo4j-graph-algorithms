package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class MSTPrimExporter extends Exporter<MSTPrim.MinimumSpanningTree> {

    private IdMapping idMapping;
    private int relationshipId = -1;

    public MSTPrimExporter(GraphDatabaseAPI api) {
        super(api);
    }

    public MSTPrimExporter withWriteRelationship(String relationship) {
        relationshipId = getOrCreateRelationshipId(relationship);
        return this;
    }

    public MSTPrimExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    @Override
    public void write(MSTPrim.MinimumSpanningTree data) {

        if (relationshipId == -1) {
            throw new IllegalArgumentException("relationshipId is not set");
        }

        writeInTransaction(write -> {
            data.forEachBFS((source, target, relationship) -> {
                try {
                    write.relationshipCreate(relationshipId,
                            idMapping.toOriginalNodeId(source),
                            idMapping.toOriginalNodeId(target));
                } catch (RelationshipTypeIdNotFoundKernelException | EntityNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        });
    }
}
