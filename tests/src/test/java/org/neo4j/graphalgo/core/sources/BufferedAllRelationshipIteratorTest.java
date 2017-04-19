package org.neo4j.graphalgo.core.sources;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BufferedAllRelationshipIteratorTest {

    private static BufferedAllRelationshipIterator iterator;

    @Mock
    private RelationshipConsumer relationConsumer;

    @BeforeClass
    public static void setupGraph() {

        iterator = BufferedAllRelationshipIterator.builder()
                .add(0, 1)
                .add(0, 2)
                .add(1, 2)
                .build();
    }

    @Test
    public void testRelations() throws Exception {
        iterator.forEachRelationship(relationConsumer);
        verify(relationConsumer, times(3)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(0), eq(1), anyLong());
        verify(relationConsumer, times(1)).accept(eq(0), eq(2), anyLong());
        verify(relationConsumer, times(1)).accept(eq(1), eq(2), anyLong());
    }
}
