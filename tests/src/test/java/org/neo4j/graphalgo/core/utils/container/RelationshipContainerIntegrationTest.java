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
package org.neo4j.graphalgo.core.utils.container;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationshipContainerIntegrationTest extends Neo4JTestCase {

    private static RelationshipContainer container;

    private static int a, b, c;

    @Mock
    private RelationshipConsumer consumer;

    @BeforeClass
    public static void buildGraph() {

        a = newNode();
        b = newNode();
        c = newNode();

        newRelation(a, b);
        newRelation(a, c);
        newRelation(b, c);

        final LazyIdMapper idMapper = new LazyIdMapper(3);

        container = RelationshipContainer.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withDirection(Direction.OUTGOING)
                .build();

        a = idMapper.toMappedNodeId(a);
        b = idMapper.toMappedNodeId(b);
        c = idMapper.toMappedNodeId(c);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @Test
    public void testV0ForEach() throws Exception {
        container.forEach(a, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(b), eq(-1L));
        verify(consumer, times(1)).accept(eq(a), eq(c), eq(-1L));
    }

    @Test
    public void testV1ForEach() throws Exception {
        container.forEach(b, consumer);
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(c), eq(-1L));
    }

    @Test
    public void testVXForEach() throws Exception {
        container.forEach(42, consumer);
        verify(consumer, never()).accept(anyInt(), anyInt(), anyLong());
    }
}
