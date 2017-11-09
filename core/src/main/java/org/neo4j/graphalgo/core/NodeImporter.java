/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
