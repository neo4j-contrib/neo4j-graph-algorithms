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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;

import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BothRelationshipAdapterTest {

    @Mock
    private RelationshipConsumer consumer;

    private static int a = 0, b = 1, c = 2;
    private static BothRelationshipAdapter bothRelationshipAdapter;

    @BeforeClass
    public static void setupGraph() {
        RelationshipContainer relationshipContainer = RelationshipContainer.builder(3)
                .aim(a, 2)
                .add(b)
                .add(c)
                .aim(b, 2)
                .add(a)
                .add(c)
                .aim(c, 2)
                .add(a)
                .add(b)
                .build();

        bothRelationshipAdapter = new BothRelationshipAdapter(relationshipContainer);
    }

    @Before
    public void setupMocks() {
        when(consumer.accept(anyInt(), anyInt(), anyLong())).thenReturn(true);
    }

    @Test
    public void testFromA() throws Exception {
        bothRelationshipAdapter.forEachRelationship(a, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(b), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(c), anyLong());
    }

    @Test
    public void testFromB() throws Exception {
        bothRelationshipAdapter.forEachRelationship(b, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(a), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(c), anyLong());
    }

    @Test
    public void testFromC() throws Exception {
        bothRelationshipAdapter.forEachRelationship(c, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(c), eq(a), anyLong());
        verify(consumer, times(1)).accept(eq(c), eq(b), anyLong());
    }
}
