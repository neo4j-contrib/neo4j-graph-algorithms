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
package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.IntLongScatterMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntScatterMap;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class LazyIdMapper implements IdMapping {

    private final LongIntMap forward;
    private final IntLongMap backward;
    private final int nodeCount;

    private int current = 0;

    /**
     * initialize without predefined size so that
     * {@link LazyIdMapper#nodeCount()} returns the
     * current number of objects
     */
    public LazyIdMapper() {
        forward = new LongIntScatterMap();
        backward = new IntLongScatterMap();
        this.nodeCount = -1;
    }

    /**
     * initialize the id-map with a predefined node count.
     *
     * @param nodeCount the number of nodes to map
     */
    public LazyIdMapper(int nodeCount) {
        forward = new LongIntScatterMap(nodeCount);
        backward = new IntLongScatterMap(nodeCount);
        this.nodeCount = nodeCount;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        int value = forward.getOrDefault(nodeId, -1);
        if (value != -1) {
            return value;
        }
        forward.put(nodeId, current);
        backward.put(current, nodeId);
        return current++;
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return backward.getOrDefault(nodeId, -1);
    }

    @Override
    public boolean contains(final long nodeId) {
        return forward.containsKey(nodeId);
    }

    @Override
    public long nodeCount() {
        return nodeCount == -1 ? forward.size() : nodeCount;
    }

    public static LazyIdMapperImporter importer(GraphDatabaseAPI api) {
        return new LazyIdMapperImporter(api);
    }

    public static class LazyIdMapperImporter extends Importer<LazyIdMapper, LazyIdMapperImporter> {

        public LazyIdMapperImporter(GraphDatabaseAPI api) {
            super(api);
        }

        @Override
        protected LazyIdMapperImporter me() {
            return this;
        }

        @Override
        protected LazyIdMapper buildT() {
            return new LazyIdMapper(nodeCount);
        }
    }
}
