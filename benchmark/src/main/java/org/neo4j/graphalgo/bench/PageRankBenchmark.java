package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.impl.PageRank;
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
import org.openjdk.jmh.annotations.TearDown;
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

    @Param({"5", "20", "100"})
    int iterations;

    @Param({"LIGHT", "HEAVY", "VIEW"})
    GraphImpl impl;

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
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
    }

    @Benchmark
    public double[] run() throws Exception {
        final Graph graph = new GraphLoader(db).load(impl.impl);
        try {
            PageRank pageRank = new PageRank(
                    graph,
                    graph,
                    graph,
                    graph,
                    0.85);
            return pageRank.compute(iterations).getPageRank();
        } finally {
            if (graph instanceof AutoCloseable) {
                ((AutoCloseable) graph).close();
            }
        }
    }
}
