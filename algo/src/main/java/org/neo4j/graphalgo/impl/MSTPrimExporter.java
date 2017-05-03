package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class MSTPrimExporter extends Exporter<MSTPrim.MinimumSpanningTree> {

    private RelationshipType type;
    private IdMapping idMapping;

    public MSTPrimExporter(GraphDatabaseAPI api) {
        super(api);
    }

    public MSTPrimExporter withWriteRelationship(String relationship) {
        type = RelationshipType.withName(relationship);
        return this;
    }

    public MSTPrimExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    @Override
    public void write(MSTPrim.MinimumSpanningTree data) {
        writeInTransaction(write -> {
            data.forEachBFS((source, target, relationship) -> {
                Node sourceNode = api.getNodeById(idMapping.toOriginalNodeId(source));
                Node targetNode = api.getNodeById(idMapping.toOriginalNodeId(target));
                sourceNode.createRelationshipTo(targetNode, type);
                return true;
            });
        });
    }
}
