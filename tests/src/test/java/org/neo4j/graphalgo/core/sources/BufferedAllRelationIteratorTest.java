package org.neo4j.graphalgo.core.sources;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.IntConsumer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BufferedAllRelationIteratorTest {

    private static BufferedAllRelationIterator iterator;

    @Mock
    private RelationConsumer relationConsumer;

    @BeforeClass
    public static void setupGraph() {
        iterator = new BufferedAllRelationIterator(3);
        iterator.add(0, 1);
        iterator.add(0, 2);
        iterator.add(1, 2);

    }

    @Test
    public void testRelations() throws Exception {
        iterator.forEachRelation(relationConsumer);
        verify(relationConsumer, times(3)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(0), eq(1), anyLong());
        verify(relationConsumer, times(1)).accept(eq(0), eq(2), anyLong());
        verify(relationConsumer, times(1)).accept(eq(1), eq(2), anyLong());
    }
}
