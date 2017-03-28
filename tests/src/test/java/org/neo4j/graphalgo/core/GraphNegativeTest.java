package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationCursor;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public final class GraphNegativeTest extends RandomGraphTestCase {

    private Class<? extends GraphFactory> graphImpl;

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{LightGraphFactory.class, "LightGraphFactory"}
        );
    }

    @SuppressWarnings("unchecked")
    public GraphNegativeTest(
            Class<?> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = (Class<? extends GraphFactory>) graphImpl;
    }

    @Test
    public void shouldLoadAllNodesForNonExistingStringLabel() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withLabel("foo")
                .load(graphImpl);
        assertEquals(graph.nodeCount(), NODE_COUNT);
    }

    @Test
    public void shouldLoadAllNodesForNonExistingLabel() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withLabel(Label.label("foo"))
                .load(graphImpl);
        assertEquals(graph.nodeCount(), NODE_COUNT);
    }

    @Test
    public void shouldLoadRelationshipsNodesForNonExistingStringTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType("foo")
                .load(graphImpl);
        testRelationships(graph);
    }

    @Test
    public void shouldLoadRelationshipsNodesForNonExistingTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType(RelationshipType.withName("foo"))
                .load(graphImpl);
        testRelationships(graph);
    }

    @Test
    public void shouldLoadDefaultWeightForNonExistingProperty() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withWeightsFromProperty("foo", 13.37)
                .load(graphImpl);
        graph.forEachNode(node -> graph.forEachRelation(
                node,
                Direction.OUTGOING,
                (start, end, rel, weight) ->
                        assertEquals(13.37, weight, 0.0001)));
    }

    private void testRelationships(final Graph graph) {
        try (Transaction tx = db.beginTx()) {
            final ResourceIterable<Relationship> rels = db.getAllRelationships();
            ResourceIterator<Relationship> iterator = rels.iterator();
            while (iterator.hasNext()) {
                final Relationship rel = iterator.next();
                final String rs = String.format(
                        "(%s)-[%s]->(%s)",
                        rel.getStartNode().getId(),
                        rel.getId(),
                        rel.getEndNode().getId());
                boolean hasRelation = false;
                final Iterator<RelationCursor> relIter = graph.relationIterator(
                        graph.toMappedNodeId(rel.getStartNode().getId()),
                        Direction.OUTGOING);
                while (relIter.hasNext()) {
                    RelationCursor next = relIter.next();
                    if (next.relationId == rel.getId()) {
                        hasRelation = true;
                        assertEquals(
                                rs,
                                graph.toOriginalNodeId(next.targetNodeId),
                                rel.getEndNode().getId()
                        );
                    }
                }
                assertTrue(
                        "did not find relation " + rs,
                        hasRelation);
            }
            iterator.close();
            tx.success();
        }
    }
}
