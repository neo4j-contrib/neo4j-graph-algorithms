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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class SingleRunRelationIteratorTest extends Neo4JTestCase {

    private static int v0, v1, v2;

    private static SingleRunAllRelationIterator iterator;
    static final SimpleGraphSetup setup = new SimpleGraphSetup();

    @Mock
    private RelationshipConsumer relationConsumer;

    @BeforeClass
    public static void setupGraph() {
        LazyIdMapper idMapper = new LazyIdMapper(3);
        iterator = new SingleRunAllRelationIterator((GraphDatabaseAPI) setup.getDb(), idMapper);
        v0 = idMapper.toMappedNodeId(setup.getN0());
        v1 = idMapper.toMappedNodeId(setup.getN1());
        v2 = idMapper.toMappedNodeId(setup.getN2());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        setup.getDb().shutdown();
        if (db != null) db.shutdown();
    }

    @Test
    public void testRelations() throws Exception {
        iterator.forEachRelationship(relationConsumer);
        verify(relationConsumer, times(3)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v2), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong());
    }
}
