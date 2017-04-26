package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public final class GraphNegativeTest extends RandomGraphTestCase {

    private Class<? extends GraphFactory> graphImpl;

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{LightGraphFactory.class, "LightGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
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
    public void shouldLoadWeightedRelationshipsNodesForNonExistingStringTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType("foo")
                .withRelationshipWeightsFromProperty("weight", 42.0)
                .load(graphImpl);
        testWeightedRelationships(graph);
    }

    @Test
    public void shouldLoadWeightedRelationshipsNodesForNonExistingTypes() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipType(RelationshipType.withName("foo"))
                .withRelationshipWeightsFromProperty("weight", 42.0)
                .load(graphImpl);
        testWeightedRelationships(graph);
    }

    @Test
    public void shouldLoadDefaultWeightForNonExistingProperty() {
        final Graph graph = new GraphLoader(RandomGraphTestCase.db)
                .withRelationshipWeightsFromProperty("foo", 13.37)
                .load(graphImpl);
        graph.forEachNode(node -> graph.forEachRelationship(
                node,
                Direction.OUTGOING,
                (start, end, rel, weight) -> {
                        assertEquals(13.37, weight, 0.0001);
                        return true;
                }));
    }

    private void testRelationships(final Graph graph) {
        testAnyRelationship(graph, (rs, rel) -> Pair.of(
                graph.relationshipIterator(
                        graph.toMappedNodeId(rel.getStartNode().getId()),
                        Direction.OUTGOING),
                cursor -> {}
        ));
    }

    private void testWeightedRelationships(final Graph graph) {
        testAnyRelationship(graph, (rs, rel) -> {
            double weight = ((Number) rel.getProperty("weight")).doubleValue();
            return Pair.of(
                    graph.weightedRelationshipIterator(
                            graph.toMappedNodeId(rel.getStartNode().getId()),
                            Direction.OUTGOING),
                    (cursor) -> assertEquals(
                            rs + " wrong weight",
                            weight,
                            cursor.weight,
                            0.001
                    )
            );
        });
    }

    private <T extends RelationshipCursor> void testAnyRelationship(
            Graph graph,
            BiFunction<String, Relationship, Pair<Iterator<T>, Consumer<T>>> tester) {
        try (Transaction tx = db.beginTx()) {
            ResourceIterable<Relationship> rels = db.getAllRelationships();
            ResourceIterator<Relationship> iterator = rels.iterator();
            while (iterator.hasNext()) {
                Relationship rel = iterator.next();
                String rs = String.format(
                        "(%s)-[%s]->(%s)",
                        rel.getStartNode().getId(),
                        rel.getId(),
                        rel.getEndNode().getId());
                Pair<Iterator<T>, Consumer<T>> test =
                        tester.apply(rs, rel);

                boolean hasRelation = false;
                Iterator<T> relIter = test.first();
                while (relIter.hasNext()) {
                    T cursor = relIter.next();
                    if (cursor.relationshipId == rel.getId()) {
                        hasRelation = true;
                        assertEquals(
                                rs,
                                rel.getEndNode().getId(),
                                graph.toOriginalNodeId(cursor.targetNodeId)
                        );
                        test.other().accept(cursor);
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
