package org.neo4j.graphalgo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

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
@RunWith(MockitoJUnitRunner.class)
public class GraphTest extends Neo4JTestCase {

    private static Graph graph;
    private static int v0, v1, v2;

    @Mock
    private WeightedRelationConsumer weightedRelationConsumer;

    @Mock
    private WeightedRelationSink weightedRelationSink;

    @Mock
    private RelationConsumer relationConsumer;

    @Mock
    private RelationSink relationSink;

    @Mock
    private IntConsumer nodeConsumer;


    @BeforeClass
    public static void buildGraph() throws Exception {

        v0 = newNode();
        v1 = newNode();
        v2 = newNode();

        newRelation(v0, v1, 1.0);
        newRelation(v0, v2, 2.0);
        newRelation(v1, v2, 3.0);

        graph = neo4jView();
    }

    public void setupMock() {
        Mockito.reset(
                relationConsumer,
                relationSink,
                weightedRelationConsumer,
                weightedRelationSink,
                nodeConsumer);
    }

    @Test
    public void testForEachNode() throws Exception {

        graph.forEachNode(nodeConsumer);
        verify(nodeConsumer, times(3)).accept(anyInt());
        verify(nodeConsumer, times(1)).accept(eq(v0));
        verify(nodeConsumer, times(1)).accept(eq(v1));
        verify(nodeConsumer, times(1)).accept(eq(v2));
    }

    @Test
    public void testNodeIterator() throws Exception {
        final PrimitiveIntIterator iterator = graph.nodeIterator();
        while(iterator.hasNext()) {
            nodeConsumer.accept(iterator.next());
        }
        verify(nodeConsumer, times(1)).accept(eq(v0));
        verify(nodeConsumer, times(1)).accept(eq(v1));
        verify(nodeConsumer, times(1)).accept(eq(v2));
    }

    @Test
    public void testV0OutgoingForEach() throws Exception {
        graph.forEachRelation(v0, OUTGOING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v2), anyLong());
    }

    @Test
    public void testV1OutgoingForEach() throws Exception {
        graph.forEachRelation(v1, OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong());
    }

    @Test
    public void testV2OutgoingForEach() throws Exception {
        graph.forEachRelation(v2, OUTGOING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0IncomingForEach() throws Exception {
        graph.forEachRelation(v0, INCOMING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV1IncomingForEach() throws Exception {
        graph.forEachRelation(v1, INCOMING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong());
    }

    @Test
    public void testV2IncomingForEach() throws Exception {
        graph.forEachRelation(v2, INCOMING, relationConsumer);
        verify(relationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v0), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v1), anyLong());
    }

    @Test
    public void testV0OutgoingIterator() throws Exception {
        graph.relationIterator(v0, OUTGOING).forEachRemaining(consume(relationSink));
        verify(relationSink, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationSink, times(1)).accept(eq(v0), eq(v1), anyLong());
        verify(relationSink, times(1)).accept(eq(v0), eq(v1), anyLong());
    }

    @Test
    public void testV1OutgoingIterator() throws Exception {
        graph.relationIterator(v1, OUTGOING).forEachRemaining(consume(relationSink));
        verify(relationSink, times(1)).accept(eq(v1), eq(v2), anyLong());
    }

    @Test
    public void testV2OutgoingIterator() throws Exception {
        graph.relationIterator(v2, OUTGOING).forEachRemaining(consume(relationSink));
        verify(relationSink, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0IncomingIterator() throws Exception {
        graph.relationIterator(v0, INCOMING).forEachRemaining(consume(relationSink));
        verify(relationSink, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV1IncomingIterator() throws Exception {
        graph.relationIterator(v1, INCOMING).forEachRemaining(consume(relationSink));
        verify(relationSink, times(1)).accept(eq(v1), eq(v2), anyLong());
    }

    @Test
    public void testV2IncomingIterator() throws Exception {
        graph.relationIterator(v2, INCOMING).forEachRemaining(consume(relationSink));
        verify(relationSink, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationSink, times(1)).accept(eq(v2), eq(v0), anyLong());
        verify(relationSink, times(1)).accept(eq(v2), eq(v1), anyLong());
    }

    @Test
    public void testV0OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v0, OUTGOING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, times(2)).accept(anyInt(), anyInt(), anyLong(), anyDouble());
        verify(weightedRelationSink, times(1)).accept(eq(v0), eq(v1), anyLong(), eq(1.0));
        verify(weightedRelationSink, times(1)).accept(eq(v0), eq(v2), anyLong(), eq(2.0));
    }

    @Test
    public void testV1OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v1, OUTGOING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, times(1)).accept(eq(v1), eq(v2), anyLong(), eq(3.0));
    }

    @Test
    public void testV2OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v1, OUTGOING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, never()).accept(anyInt(), anyInt(), anyLong(), anyDouble());
    }

    @Test
    public void testV0IncomingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v0, INCOMING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, never()).accept(anyInt(), anyInt(), anyLong(), anyDouble());
    }

    @Test
    public void testV1IncomingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v1, INCOMING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, times(1)).accept(eq(v1), eq(v2), anyLong(), eq(1.0));
    }

    @Test
    public void testV2IncomingWeightedIterator() throws Exception {
        graph.weightedRelationIterator(v2, INCOMING).forEachRemaining(consume(weightedRelationSink));
        verify(weightedRelationSink, times(1)).accept(eq(v2), eq(v0), anyLong(), eq(2.0));
        verify(weightedRelationSink, times(1)).accept(eq(v2), eq(v1), anyLong(), eq(3.0));
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

    private Consumer<RelationCursor> consume(RelationSink sink) {
        return c -> sink.accept(c.sourceNodeId, c.targetNodeId, c.relationId);
    }


    private Consumer<WeightedRelationCursor> consume(WeightedRelationSink sink) {
        return c -> sink.accept(c.sourceNodeId, c.targetNodeId, c.relationId, c.weight);
    }

    private interface RelationSink {
        void accept(int source, int target, long rel);
    }

    private interface WeightedRelationSink {
        void accept(int source, int target, long rel, double weight);
    }

}