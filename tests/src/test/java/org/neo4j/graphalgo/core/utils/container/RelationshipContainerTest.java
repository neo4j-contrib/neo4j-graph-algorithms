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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationshipContainerTest {

    private static RelationshipContainer container;

    @Mock
    private RelationshipConsumer consumer;

    @BeforeClass
    public static void setup() {
        container = RelationshipContainer.builder(2)
                .aim(0, 2)
                    .add(1)
                    .add(2)
                .aim(1, 1)
                    .add(2)
                .build();
    }


    @Test
    public void testV0ForEach() throws Exception {
        container.forEach(0, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(0), eq(1), eq(-1L));
        verify(consumer, times(1)).accept(eq(0), eq(2), eq(-1L));
    }

    @Test
    public void testV1ForEach() throws Exception {
        container.forEach(1, consumer);
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(1), eq(2), eq(-1L));
    }

    @Test
    public void testVXForEach() throws Exception {
        container.forEach(42, consumer);
        verify(consumer, never()).accept(anyInt(), anyInt(), anyLong());
    }
}
