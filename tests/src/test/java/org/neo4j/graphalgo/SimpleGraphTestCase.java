package org.neo4j.graphalgo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public abstract class SimpleGraphTestCase extends Neo4JTestCase {

    protected static Graph graph;

    protected static int v0, v1, v2;

    @Mock
    private WeightedRelationshipConsumer weightedRelationConsumer;

    @Mock
    private RelationshipConsumer relationConsumer;

    @Mock
    private IntConsumer nodeConsumer;

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
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v0), anyLong());
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
        graph.relationIterator(v0, OUTGOING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong());
    }

    @Test
    public void testV1OutgoingIterator() throws Exception {
        graph.relationIterator(v1, OUTGOING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong());
    }

    @Test
    public void testV2OutgoingIterator() throws Exception {
        graph.relationIterator(v2, OUTGOING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0IncomingIterator() throws Exception {
        graph.relationIterator(v0, INCOMING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV1IncomingIterator() throws Exception {
        graph.relationIterator(v1, INCOMING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, times(1)).accept(eq(v1), eq(v0), anyLong());
    }

    @Test
    public void testV2IncomingIterator() throws Exception {
        graph.relationIterator(v2, INCOMING).forEachRemaining(consume(relationConsumer));
        verify(relationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v0), anyLong());
        verify(relationConsumer, times(1)).accept(eq(v2), eq(v1), anyLong());
    }

    @Test
    public void testV0OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v0, OUTGOING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, times(2)).accept(anyInt(), anyInt(), anyLong(), anyDouble());
        verify(weightedRelationConsumer, times(1)).accept(eq(v0), eq(v1), anyLong(), eq(1.0));
        verify(weightedRelationConsumer, times(1)).accept(eq(v0), eq(v2), anyLong(), eq(2.0));
    }

    @Test
    public void testV1OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v1, OUTGOING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, times(1)).accept(eq(v1), eq(v2), anyLong(), eq(3.0));
    }

    @Test
    public void testV2OutgoingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v2, OUTGOING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, never()).accept(anyInt(), anyInt(), anyLong(), anyDouble());
    }

    @Test
    public void testV0IncomingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v0, INCOMING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, never()).accept(anyInt(), anyInt(), anyLong(), anyDouble());
    }

    @Test
    public void testV1IncomingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v1, INCOMING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, times(1)).accept(eq(v1), eq(v0), anyLong(), eq(1.0));
    }

    @Test
    public void testV2IncomingWeightedIterator() throws Exception {
        graph.weightedRelationshipIterator(v2, INCOMING).forEachRemaining(consume(weightedRelationConsumer));
        verify(weightedRelationConsumer, times(1)).accept(eq(v2), eq(v0), anyLong(), eq(2.0));
        verify(weightedRelationConsumer, times(1)).accept(eq(v2), eq(v1), anyLong(), eq(3.0));
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

    private Consumer<RelationshipCursor> consume(RelationshipConsumer sink) {
        return c -> sink.accept(c.sourceNodeId, c.targetNodeId, c.relationId);
    }


    private Consumer<WeightedRelationshipCursor> consume(WeightedRelationshipConsumer sink) {
        return c -> sink.accept(c.sourceNodeId, c.targetNodeId, c.relationId, c.weight);
    }

}