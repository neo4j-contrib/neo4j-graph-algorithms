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

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;
import scala.reflect.ClassTag;
import xerial.larray.LArray;
import xerial.larray.LArray$;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TriangleCountBenchmark {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    public static final long TRIANGLE_COUNT = 1000L;

    private static Graph graph;
    private static LArray<long[]> adjacencyListGraph;
    private static GraphDatabaseAPI api;

    protected static LArray<long[]> createAdjacencyListGraph(Graph g) {
        LArray<long[]> adjGraph = createGraphLArray(Long.valueOf(graph.nodeCount()));
        LongArrayList lal = new LongArrayList();
        g.forEachNode((id) -> {
            g.forEachRelationship(id, Direction.BOTH, (u, v, relationId) -> {
                lal.add(Long.valueOf(v));
                return true;
            });
            long[] adjAsArray = lal.toArray();
            Arrays.parallelSort(adjAsArray);
            adjGraph.update(id, adjAsArray);
            lal.clear();
            return true;
        });
        return adjGraph;
    }

    protected static LArray<long[]> createGraphLArray(Long size) {
        ClassTag<long[]> ct = scala.reflect.ClassTag$.MODULE$.apply(long[].class);
        return LArray$.MODULE$.of(size, ct);
    }

    @Param({"1", "2", "4", "8"})
    int threads;

    @Setup
    public static void setup() throws KernelException {
        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + TRIANGLE_COUNT + " nodes"))) {

            final RelationshipType type = RelationshipType.withName(RELATIONSHIP);
            final DefaultBuilder builder = GraphBuilder.create(api)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newDefaultBuilder();
            final Node center = builder.createNode();
            builder.newRingBuilder()
                    .createRing((int) TRIANGLE_COUNT)
                    .forEachNodeInTx(node -> {
                        center.createRelationshipTo(node, type);
                    });
        };

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(api)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .load(HeavyGraphFactory.class);
            adjacencyListGraph = createAdjacencyListGraph(graph);
        };
    }


    @TearDown
    public static void tearDown() throws Exception {
        if (api != null) api.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _01_triangleCount() {
        return new TriangleCount(graph, Pools.DEFAULT, threads)
                .compute()
                .getTriangleCount();
    }

    @Benchmark
    public Object _02_triangleCountExp() {
        return new TriangleCountExp(graph, Pools.DEFAULT, threads)
                .compute()
                .getTriangleCount();
    }

    @Benchmark
    public Object _03_triangleCountOptimized() {
        return new Triangles(adjacencyListGraph, ForkJoinPool.commonPool(), 10000)
                .count();
    }

    @Benchmark
    public Object _03_triangleFindOptimized() {
        TriangleIterator i = new Triangles(adjacencyListGraph, ForkJoinPool.commonPool(), 10000)
                .find();
        long count = 0;
        while (i.hasNext()) {
            i.next();
            count += 1;
        }
        return count;
    }

}
