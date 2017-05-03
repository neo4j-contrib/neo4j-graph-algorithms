package org.neo4j.graphalgo.core.graphbuilder;

import org.junit.Test;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.kernel.internal.GraphDatabaseAPI;


import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
public class GraphBuilderTest extends Neo4JTestCase{

    @Test
    public void testRingBuilder() throws Exception {

        Runnable mock = mock(Runnable.class);
        GraphBuilder.create((GraphDatabaseAPI) db)
                .newRingBuilder()
                .setRelationship(RELATION)
                .createRing(10)
                .forEachNodeInTx(node -> {
                    assertEquals(2, node.getDegree());
                    mock.run();
                });
        verify(mock, times(10)).run();

    }
}