package org.neo4j.graphalgo.bench;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.sources.BufferedAllRelationshipIterator;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.graphalgo.impl.ParallelUnionFind;
import org.neo4j.graphalgo.impl.UnionFind;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

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
public class ParallelUnionFindBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private GraphDatabaseAPI db;

    private Graph graph;

    @Setup
    public void setup() {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        // creates disconnected graph with 4 connected subgraphs
        createTestGraph(10, 11, 12, 13, 14, 15, 16, 17);

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);
    }

    private void createTestGraph(int... setSizes) {
        try (Transaction tx = db.beginTx()) {
            for (int setSize : setSizes) {
                createLine(setSize);
            }
            tx.success();
        }
    }

    private void createLine(int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();
            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }
    }

    @Benchmark
    public Object _01_parallelUnionFind() {
        return new ParallelUnionFind(graph, Pools.DEFAULT, 20)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object _02_sequentialUnionFind() {
        return new GraphUnionFind(graph)
                .compute();

    }

}
