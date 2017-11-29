/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author mknblch
 */
public class TriangleCountExpTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final long TRIANGLE_COUNT = 1000L;
    public static final double EXPECTED_COEFFICIENT = 0.666;

    private static GraphDatabaseAPI db;
    private static Graph graph;
    private static long centerId;

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + TRIANGLE_COUNT + " nodes"))) {

            final RelationshipType type = RelationshipType.withName(RELATIONSHIP);
            final DefaultBuilder builder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newDefaultBuilder();
            final Node center = builder.createNode();
            builder.newRingBuilder()
                    .createRing((int) TRIANGLE_COUNT)
                    .forEachNodeInTx(node -> {
                        center.createRelationshipTo(node, type);
                    });
            centerId = center.getId();
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withSort(true)
                    .load(HeavyGraphFactory.class);
        };
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        graph = null;
    }

    @Test
    public void testExp2() throws Exception {
        final TriangleCountExp2 algo = new TriangleCountExp2(graph, Pools.DEFAULT, 1);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("took " + l + "ms"))) {
            algo.compute();
        }
        assertEquals(TRIANGLE_COUNT, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
        assertClusteringCoefficient(algo.getClusteringCoefficients());
        assertEquals(EXPECTED_COEFFICIENT, algo.getAverageClusteringCoefficient(), 0.001);
    }

    @Test
    public void testExp2Parallel() throws Exception {
        final TriangleCountExp2 algo = new TriangleCountExp2(graph, Pools.DEFAULT, 4);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("took " + l + "ms"))) {
            algo.compute();
        }
        assertEquals(TRIANGLE_COUNT, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
        assertClusteringCoefficient(algo.getClusteringCoefficients());
        assertEquals(EXPECTED_COEFFICIENT, algo.getAverageClusteringCoefficient(), 0.001);
    }

    @Test
    public void testExp3() throws Exception {
        final TriangleCountExp3 algo = new TriangleCountExp3(graph, ForkJoinPool.commonPool(), 100);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("took " + l + "ms"))) {
            algo.compute(true);
        }
        assertEquals(TRIANGLE_COUNT, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
        assertClusteringCoefficient(algo.getClusteringCoefficients());
        assertEquals(EXPECTED_COEFFICIENT, algo.getAverageClusteringCoefficient(), 0.001);
    }

    @Test
    public void testSequential() throws Exception {
        final TriangleCountExp algo = new TriangleCountExp(graph, Pools.DEFAULT, 1);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("sequential count took " + l + "ms"))) {
            algo.compute();
        }
        assertTriangles(algo.getTriangles());
        assertClusteringCoefficient(algo.getClusteringCoefficients());
        assertEquals(TRIANGLE_COUNT, (long) algo.getTriangleCount());
        assertEquals(EXPECTED_COEFFICIENT, algo.getAverageClusteringCoefficient(), 0.001);
    }

    @Test
    public void testParallel() throws Exception {
        final TriangleCountExp algo = new TriangleCountExp(graph, Pools.DEFAULT, 4);
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("parallel eval took " + l + "ms"))) {
            algo.compute();
        }
        assertClusteringCoefficient(algo.getClusteringCoefficients());
        assertEquals(TRIANGLE_COUNT, (long) algo.getTriangleCount());
        assertTriangles(algo.getTriangles());
        assertEquals(EXPECTED_COEFFICIENT, algo.getAverageClusteringCoefficient(), 0.001);
    }

    private void assertTriangles(AtomicIntegerArray triangles) {
        final int centerMapped = graph.toMappedNodeId(centerId);
        assertEquals(TRIANGLE_COUNT, triangles.get(centerMapped));
        for (int i = 0; i < triangles.length(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(2, triangles.get(i));
        }
    }

    private void assertClusteringCoefficient(double[] coefficients) {
        final int centerMapped = graph.toMappedNodeId(centerId);
        for (int i = 0; i < coefficients.length; i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(EXPECTED_COEFFICIENT, coefficients[i], 0.01);
        }
    }

    private void assertClusteringCoefficient(AtomicDoubleArray coefficients) {
        final int centerMapped = graph.toMappedNodeId(centerId);
        for (int i = 0; i < coefficients.length(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(EXPECTED_COEFFICIENT, coefficients.get(i), 0.01);
        }
    }
}
