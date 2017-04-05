package org.neo4j.graphalgo.core.utils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.function.IntConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 *  0 -- 2 -- 6 -- 8
 *       |
 *       4
 *
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class DirectedGraphTest {

    private DirectedGraph graph;

    @Mock
    private IntConsumer intConsumer;

    @Before
    public void setup() throws Exception {

        graph = new DirectedGraph(1, 1);

        graph.connect(0, 2, 0);
        graph.connect(2, 4, 1);
        graph.connect(2, 6, 2);
        graph.connect(6, 8, 3);
    }

    @Test
    public void testDegree() throws Exception {
        assertEquals(5, graph.nodeCount());

        assertEquals(1, graph.degree(0));
        assertEquals(2, graph.degree(2));
        assertEquals(0, graph.degree(4));
        assertEquals(1, graph.degree(6));
        assertEquals(0, graph.degree(8));
    }

    @Test
    public void testNodeIterator() throws Exception {
        for (PrimitiveIntIterator it = graph.nodeIterator(); it.hasNext(); ) {
            intConsumer.accept(it.next());
        }
        verify(intConsumer, times(1)).accept(0);
        verify(intConsumer, times(1)).accept(2);
        verify(intConsumer, times(1)).accept(4);
        verify(intConsumer, times(1)).accept(6);
        verify(intConsumer, times(1)).accept(8);
    }
}
