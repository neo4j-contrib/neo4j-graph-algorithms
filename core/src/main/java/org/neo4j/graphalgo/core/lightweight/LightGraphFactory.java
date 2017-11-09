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
package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class LightGraphFactory extends GraphFactory {

    public LightGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        try {
            return importGraph();
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }

    private Graph importGraph() throws EntityNotFoundException {
        final IdMap idMap = loadIdMap();
        final GraphImporter graphImporter = new GraphImporter(
                api,
                setup,
                idMap,
                newWeightMap(dimensions.weightId(), setup.relationDefaultWeight),
                dimensions.nodeCount(),
                dimensions.relationId()
        );
        return graphImporter.call();
    }
}
