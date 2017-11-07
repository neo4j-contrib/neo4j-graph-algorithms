package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.IntArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

final class GraphImporter extends StatementTask<LightGraph, EntityNotFoundException> {

    private final GraphSetup setup;
    private final IdMap mapping;
    private final WeightMapping weights;

    private final int nodeCount;
    private final int[] relationId;

    GraphImporter(
            GraphDatabaseAPI api,
            GraphSetup setup,
            IdMap mapping,
            WeightMapping weights,
            int nodeCount,
            int[] relationId) {
        super(api);
        this.setup = setup;
        this.mapping = mapping;
        this.weights = weights;
        this.nodeCount = nodeCount;
        this.relationId = relationId;
    }

    @Override
    public LightGraph apply(final Statement statement) throws EntityNotFoundException {
        long[] inOffsets = null;
        long[] outOffsets = null;
        IntArray inAdjacency = null;
        IntArray outAdjacency = null;

        boolean loadIncoming = setup.loadIncoming;
        boolean loadOutgoing = setup.loadOutgoing;
        if (loadIncoming || loadOutgoing) {
            final ReadOperations readOp = statement.readOperations();

            final int nodeCount = this.nodeCount;
            RelationshipImporter inImporter = null;
            RelationshipImporter outImporter = null;

            // we allocate one more offset in order to avoid having to
            // check for the last element during degree access
            if (loadIncoming) {
                inOffsets = new long[nodeCount + 1];
                inAdjacency = IntArray.newArray(nodeCount, AllocationTracker.EMPTY);
                inImporter = RelationshipImporter.of(
                        readOp,
                        mapping,
                        inOffsets,
                        relationId,
                        Direction.INCOMING,
                        inAdjacency.newBulkAdder(),
                        weights,
                        loadOutgoing ? RawValues.BOTH : RawValues.OUTGOING);
            }
            if (loadOutgoing) {
                outOffsets = new long[nodeCount + 1];
                outOffsets[nodeCount] = nodeCount;
                outAdjacency = IntArray.newArray(nodeCount, AllocationTracker.EMPTY);
                outImporter = RelationshipImporter.of(
                        readOp,
                        mapping,
                        outOffsets,
                        relationId,
                        Direction.OUTGOING,
                        outAdjacency.newBulkAdder(),
                        weights,
                        loadIncoming ? RawValues.BOTH : RawValues.OUTGOING);
            }

            final RelationshipImport importer = RelationshipImport.combine(outImporter, inImporter);
            final long[] ids = mapping.mappedIds();
            final int length = mapping.size();
            for (int i = 0; i < length; i++) {
                importer.importRelationships(i, ids[i]);
            }

            if (loadIncoming) {
                inOffsets[nodeCount] = inImporter.adjacencyIdx();
            }
            if (loadOutgoing) {
                outOffsets[nodeCount] = outImporter.adjacencyIdx();
            }
        }

        return new LightGraph(
                mapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }
}
