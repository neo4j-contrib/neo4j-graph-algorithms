package org.neo4j.graphalgo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.helper.IntIteratorSink;
import org.neo4j.graphalgo.helper.RelationIteratorSink;
import org.neo4j.graphalgo.helper.WeightedRelationIteratorSink;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * @author mknobloch
 */
public class WeightedGraphViewTest extends Neo4JTestCase {

    private static Graph graph;
    private static int v0, v1, v2;


    @BeforeClass
    public static void buildGraph() throws Exception {

        v0 = newNode();
        v1 = newNode();
        v2 = newNode();

        newRelation(v0, v1, 1.0);
        newRelation(v0, v2, 2.0);
        newRelation(v1, v2, 3.0);

        graph = heavyGraph();
    }

    @Test
    public void testForEachVertex() throws Exception {
        final IntIteratorSink sink = new IntIteratorSink();
        graph.forEachNode(sink);
        sink.assertContainsAll(v0, v1, v2);
    }

    @Test
    public void testForEachOutgoingEdge() throws Exception {
        final RelationIteratorSink sink = new RelationIteratorSink();

        graph.forEachRelation(v0, OUTGOING, sink);
        sink.assertContains(v0, v1)
                .reset();

        graph.forEachRelation(v1, OUTGOING, sink);
        sink.assertContains(v1, v2)
                .reset();

        graph.forEachRelation(v2, OUTGOING, sink);
        sink.assertSize(0)
                .reset();
    }

    @Test
    public void testForEachIncomingEdge() throws Exception {


        final RelationConsumer sink = mock(RelationConsumer.class);
        graph.forEachRelation(v0, INCOMING, sink);
        verify(sink, never()).accept(anyInt(), anyInt(), anyLong());
        Mockito.reset(sink);

        graph.forEachRelation(v1, INCOMING, sink);
        verify(sink, times(1)).accept(eq(v1), eq(v2), anyLong());
        Mockito.reset(sink);

        graph.forEachRelation(v2, INCOMING, sink);
        verify(sink, times(1)).accept(eq(v2), eq(v0), anyLong());
        verify(sink, times(1)).accept(eq(v2), eq(v1), anyLong());
    }

    @Test
    public void testOutgoingEdgeIterators() throws Exception {
        final RelationIteratorSink sink = new RelationIteratorSink();

        sink.consume(graph.relationIterator(v0, OUTGOING))
                .assertContains(v0, v1)
                .assertContains(v0, v2)
                .assertSize(2)
                .reset();

        sink.consume(graph.relationIterator(v1, OUTGOING))
                .assertContains(v1, v2)
                .assertSize(1)
                .reset();

        sink.consume(graph.relationIterator(v2, OUTGOING))
                .assertSize(0)
                .reset();
    }


    @Test
    public void testIncomingEdgeIterators() throws Exception {
        final RelationIteratorSink sink = new RelationIteratorSink();

        sink.consume(graph.relationIterator(v0, INCOMING))
                .assertSize(0)
                .reset();

        sink.consume(graph.relationIterator(v1, INCOMING))
                .assertContains(v1, v0)
                .assertSize(1)
                .reset();

        sink.consume(graph.relationIterator(v2, INCOMING))
                .assertContains(v2, v0)
                .assertContains(v2, v1)
                .assertSize(2)
                .reset();
    }



    @Test
    public void testOutgoingWeigehtdEdgeIterators() throws Exception {
        final WeightedRelationIteratorSink sink = new WeightedRelationIteratorSink();

        sink.consume(graph.weightedRelationIterator(v0, OUTGOING))
                .assertContains(v0, v1, 1.0)
                .assertContains(v0, v2, 2.0)
                .assertSize(2)
                .reset();

        sink.consume(graph.weightedRelationIterator(v1, OUTGOING))
                .assertContains(v1, v2, 3.0)
                .assertSize(1)
                .reset();

        sink.consume(graph.weightedRelationIterator(v2, OUTGOING))
                .assertSize(0)
                .reset();
    }

    @Test
    public void testIncomingWeigehtdEdgeIterators() throws Exception {
        final WeightedRelationIteratorSink sink = new WeightedRelationIteratorSink();

        sink.consume(graph.weightedRelationIterator(v0, INCOMING))
                .assertSize(0)
                .reset();

        sink.consume(graph.weightedRelationIterator(v1, INCOMING))
                .assertContains(v1, v0, 1.0)
                .assertSize(1)
                .reset();

        sink.consume(graph.weightedRelationIterator(v2, INCOMING))
                .assertContains(v2, v0, 2.0)
                .assertContains(v2, v1, 3.0)
                .assertSize(2)
                .reset();
    }

    @Test
    public void testVertexIterator() throws Exception {
        final IntIteratorSink sink = new IntIteratorSink();
        sink.consume(graph.nodeIterator())
                .assertContainsAll(v0, v1, v2);
    }

    @Test
    public void testDegree() throws Exception {

        assertEquals(3, graph.nodeCount());

        assertEquals(2, graph.degree(v0, OUTGOING));
        assertEquals(0, graph.degree(v0, INCOMING));

        assertEquals(1, graph.degree(v1, OUTGOING));
        assertEquals(1, graph.degree(v1, INCOMING));

        assertEquals(0, graph.degree(v2, OUTGOING));
        assertEquals(2, graph.degree(v2, INCOMING));
    }

}