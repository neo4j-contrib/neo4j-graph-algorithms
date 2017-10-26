package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class BetweennessCentralityBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;
    private static List<Node> lines = new ArrayList<>();

    @Param({"0", "1", "2", "4", "8"})
    static int concurrency;

    private static final Map<String, Object> params = new HashMap<>();

    @Setup
    public static void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);

        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("setup took " + l + "ms"))) {
            createNet(50); // size^2 nodes; size^3 edges
        }

        params.put("head", lines.get(0).getId());
        params.put("concurrency", concurrency);
    }


    @TearDown
    public static void tearDown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    private static void createNet(int size) {
        try (Transaction tx = db.beginTx()) {
            List<Node> temp = null;
            for (int i = 0; i < size; i++) {
                List<Node> line = createLine(size);
                if (null != temp) {
                    for (int j = 0; j < size; j++) {
                        for (int k = 0; k < size; k++) {
                            if (j == k) {
                                continue;
                            }
                            temp.get(j).createRelationshipTo(line.get(k), RELATIONSHIP_TYPE);
                        }
                    }
                }
                temp = line;
            }
            tx.success();
        }
    }

    private static List<Node> createLine(int length) {
        ArrayList<Node> nodes = new ArrayList<>();
        Node temp = db.createNode();
        nodes.add(temp);
        lines.add(temp);
        for (int i = 1; i < length; i++) {
            Node node = db.createNode();
            nodes.add(temp);
            temp.createRelationshipTo(node, RELATIONSHIP_TYPE);
            temp = node;
        }
        return nodes;
    }

    @Benchmark
    public Object _01_benchmark() {

        return db.execute("CALL algo.betweenness('','', {concurrency: $concurrency, write:false, stats:false}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis", params)
                .stream()
                .count();
    }

}
