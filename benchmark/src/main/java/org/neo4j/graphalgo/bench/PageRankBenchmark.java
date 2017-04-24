package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphalgo.impl.PageRankAlgo;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PageRankBenchmark {

    private Graph lightGraph, heavyGraph, neo4jView;

    @Param({"5", "20", "100"})
    int iterations;

    private GraphDatabaseAPI db;

    @Setup
    public void setup() {
        String createGraph = "CREATE (nA)\n" +
                "CREATE (nB)\n" +
                "CREATE (nC)\n" +
                "CREATE (nD)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" +
                "CREATE (nK)\n" +
                "CREATE\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nB),\n" +
                "  (nD)-[:TYPE]->(nA),\n" +
                "  (nD)-[:TYPE]->(nB),\n" +
                "  (nE)-[:TYPE]->(nB),\n" +
                "  (nE)-[:TYPE]->(nD),\n" +
                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nB),\n" +
                "  (nF)-[:TYPE]->(nE),\n" +
                "  (nG)-[:TYPE]->(nB),\n" +
                "  (nG)-[:TYPE]->(nE),\n" +
                "  (nH)-[:TYPE]->(nB),\n" +
                "  (nH)-[:TYPE]->(nE),\n" +
                "  (nI)-[:TYPE]->(nB),\n" +
                "  (nI)-[:TYPE]->(nE),\n" +
                "  (nJ)-[:TYPE]->(nE),\n" +
                "  (nK)-[:TYPE]->(nE);";
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        neo4jView = new GraphView(db, null, null, null, 1.0d);
        lightGraph = loadLight();
        heavyGraph = loadHeavy();
    }

    @Benchmark
    public Object _01_lightGraph() {
        final Graph graph = loadLight();
        return new PageRankAlgo(graph, graph, graph, graph, 0.85).compute(iterations);
    }

    @Benchmark
    public Object _02_lightGraphReuseGraph() {
        return new PageRankAlgo(lightGraph, lightGraph, lightGraph, lightGraph, 0.85).compute(iterations);
    }

    @Benchmark
    public Object _03_heavyGraph() {
        final Graph graph = loadHeavy();
        return new PageRankAlgo(graph, graph, graph, graph, 0.85).compute(iterations);
    }

    @Benchmark
    public Object _04_heavyGraphReuseGraph() {
        return new PageRankAlgo(heavyGraph, heavyGraph, heavyGraph, heavyGraph, 0.85).compute(iterations);
    }

    @Benchmark
    public Object _05_neo4jView() {
        return new PageRankAlgo(neo4jView, neo4jView, neo4jView, neo4jView,0.85).compute(iterations);
    }

    private Graph loadLight() {
        return new GraphLoader(db).load(LightGraphFactory.class);
    }

    private Graph loadHeavy() {
        return new GraphLoader(db).load(HeavyGraphFactory.class);
    }
}
