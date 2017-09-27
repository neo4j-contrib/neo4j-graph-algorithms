package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class NodeImporter extends StatementTask<IdMap, EntityNotFoundException> {
    private final ImportProgress progress;
    private final int nodeCount;
    private final int labelId;

    public NodeImporter(
            GraphDatabaseAPI api,
            ImportProgress progress,
            int nodeCount,
            int labelId) {
        super(api);
        this.progress = progress;
        this.nodeCount = nodeCount;
        this.labelId = labelId;
    }

    @Override
    protected IdMap runWithStatement(final Statement statement) throws
            EntityNotFoundException {
        final IdMap mapping = new IdMap(nodeCount);
        final ReadOperations readOp = statement.readOperations();
        final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                ? readOp.nodesGetAll()
                : readOp.nodesGetForLabel(labelId);
        while (nodeIds.hasNext()) {
            mapping.add(nodeIds.next());
            progress.nodeProgress();
        }
        mapping.buildMappedIds();
        progress.resetForRelationships();
        return mapping;
    }
}
