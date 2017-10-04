package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class HugeNodeImporter extends StatementTask<HugeIdMap, EntityNotFoundException> {
    private final AllocationTracker tracker;
    private final ImportProgress progress;
    private final long nodeCount;
    private final long allNodesCount;
    private final int labelId;

    public HugeNodeImporter(
            GraphDatabaseAPI api,
            AllocationTracker tracker,
            ImportProgress progress,
            long nodeCount,
            long allNodesCount,
            int labelId) {
        super(api);
        this.tracker = tracker;
        this.progress = progress;
        this.nodeCount = nodeCount;
        this.allNodesCount = allNodesCount;
        this.labelId = labelId;
    }

    @Override
    public HugeIdMap apply(final Statement statement) throws EntityNotFoundException {
        final HugeIdMap mapping = new HugeIdMap(nodeCount, allNodesCount, tracker);
        final ReadOperations readOp = statement.readOperations();
        final PrimitiveLongIterator nodeIds = labelId == ReadOperations.ANY_LABEL
                ? readOp.nodesGetAll()
                : readOp.nodesGetForLabel(labelId);
        while (nodeIds.hasNext()) {
            mapping.add(nodeIds.next());
            progress.nodeProgress();
        }
        progress.resetForRelationships();
        return mapping;
    }
}
