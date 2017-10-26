package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
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
public class BetweennessComparisionBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;
    private static List<Node> lines = new ArrayList<>();

    @Setup
    public static void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        final Procedures procedures = db.getDependencyResolver()
                .resolveDependency(Procedures.class);
        procedures.registerProcedure(BetweennessCentralityProc.class);

        createNet(30);
    }

    @TearDown
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
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
                            if (i == k) {
                                continue;
                            }
                            createRelation(temp.get(j), line.get(k));
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
            createRelation(temp, node);
            temp = node;
        }
        return nodes;
    }

    private static Relationship createRelation(Node from, Node to) {
        return from.createRelationshipTo(to, RELATIONSHIP_TYPE);
    }

    @Benchmark
    public Object _01_benchmark_sequential() {
        return db.execute("CALL algo.betweenness('','', {write:false}) YIELD computeMillis")
                .stream()
                .count();
    }

    @Benchmark
    public Object _04_benchmark_parallel8() {
        return db.execute("CALL algo.betweenness('','', {write:false, concurrency:8}) YIELD computeMillis")
                .stream()
                .count();
    }

    @Benchmark
    public Object _05_benchmark_sucessorBrandes() {
        return db.execute("CALL algo.betweenness.exp1('','', {write:false, concurrency:8}) YIELD computeMillis")
                .stream()
                .count();
    }

}
