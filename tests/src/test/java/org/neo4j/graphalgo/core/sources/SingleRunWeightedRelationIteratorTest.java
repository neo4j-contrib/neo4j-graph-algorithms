package org.neo4j.graphalgo.core.sources;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class SingleRunWeightedRelationIteratorTest extends Neo4JTestCase {

    private static int v0, v1, v2;

    private static SingleRunWeightedAllRelationIterator iterator;

    @Mock
    private WeightedRelationshipConsumer relationConsumer;

    @BeforeClass
    public static void setupGraph() {
        final SimpleGraphSetup setup = new SimpleGraphSetup();
        LazyIdMapper idMapper = new LazyIdMapper();
        iterator = SingleRunWeightedAllRelationIterator.importer((GraphDatabaseAPI) setup.getDb())
                .withIdMapping(idMapper)
                .withWeightsFromProperty(SimpleGraphSetup.PROPERTY, 0.0)
                .build();

        v0 = idMapper.toMappedNodeId(setup.getN0());
        v1 = idMapper.toMappedNodeId(setup.getN1());
        v2 = idMapper.toMappedNodeId(setup.getN2());
    }

    @Test
    public void testRelations() throws Exception {
        iterator.forEachRelationship(relationConsumer);
        verify(relationConsumer, times(3)).accept(anyInt(), anyInt(), anyLong(), anyDouble());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong(), eq(1.0));
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v2), anyLong(), eq(2.0));
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong(), eq(3.0));
    }
}
