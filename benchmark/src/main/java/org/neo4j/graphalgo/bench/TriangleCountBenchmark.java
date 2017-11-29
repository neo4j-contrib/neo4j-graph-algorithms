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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TriangleCountBenchmark {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final int TRIANGLE_COUNT = 500;

    private static Graph graph;
    private static GraphDatabaseAPI api;

    @Param({"0.02", "0.5", "0.8"})
    private double connecteness;

    @Setup
    public static void setup() throws KernelException {
        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + TRIANGLE_COUNT + " nodes"))) {

            GraphBuilder.create(api)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newCompleteGraphBuilder()
                    .createCompleteGraph(TRIANGLE_COUNT, 0.5);
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(api)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withSort(true)
                    .load(HeavyGraphFactory.class);
        };
    }


    @TearDown
    public static void tearDown() throws Exception {
        if (api != null) api.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

//    @Benchmark
    public Object triangleCount_singleThreaded() {
        return new TriangleCount(graph, Pools.DEFAULT, 1)
                .compute()
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCount_multiThreaded() {
        return new TriangleCount(graph, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY)
                .compute()
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCountExp_singleThreaded() {
        return new TriangleCountExp(graph, Pools.DEFAULT, 1)
                .compute()
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCountExp_multiThreaded() {
        return new TriangleCountExp(graph, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY)
                .compute()
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCountExp2_singleThreaded() {
        return new TriangleCountExp2(graph, Pools.DEFAULT, 1)
                .compute()
                .getTriangleCount();
    }

    @Benchmark
    public Object triangleCountExp2_multiThreaded() {
        return new TriangleCountExp2(graph, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY)
                .compute()
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCountExp3_singleThreaded() {
        return new TriangleCountExp3(graph, Pools.FJ_POOL, 10000)
                .compute(false)
                .getTriangleCount();
    }

    @Benchmark
    public Object triangleCountExp3_multiThreaded() {
        return new TriangleCountExp3(graph, Pools.FJ_POOL, TRIANGLE_COUNT / Pools.DEFAULT_CONCURRENCY)
                .compute(false)
                .getTriangleCount();
    }

//    @Benchmark
    public Object triangleCountExp3_Coefficients_singleThreaded() {
        return new TriangleCountExp3(graph, Pools.FJ_POOL, 10000)
                .compute(true)
                .getTriangleCount();
    }

    @Benchmark
    public Object triangleCountExp3_Coefficients_multiThreaded() {
        return new TriangleCountExp3(graph, Pools.FJ_POOL, TRIANGLE_COUNT / Pools.DEFAULT_CONCURRENCY)
                .compute(true)
                .getTriangleCount();
    }


}
