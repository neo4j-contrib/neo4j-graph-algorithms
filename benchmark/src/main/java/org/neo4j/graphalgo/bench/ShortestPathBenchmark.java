package org.neo4j.graphalgo.bench;

import algo.Pools;
import org.neo4j.graphalgo.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ShortestPathBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;
    private static List<Node> lines = new ArrayList<>();

//    @Param({"1.0", "2.0", "2.5", "3.0", "5.0", "10.0", "20.0", "50.0", "100.0"})
//    static double delta;
    static double delta = 2.5;

    @Param({"1", "2", "4", "8"})
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
                .registerProcedure(ShortestPathDeltaSteppingProc.class);
        createNet(100); // 10000 nodes; 1000000 edges
        params.put("head", lines.get(0).getId());
        params.put("delta", delta);
        params.put("concurrency", concurrency);

        db.execute("MATCH (n {id:$head}) RETURN n", params);

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
        Relationship relationship = from.createRelationshipTo(to, RELATIONSHIP_TYPE);
        double rndCost = (to.getId() % 5) + 1.0; // (0-5)
        relationship.setProperty("cost",  rndCost);
        return relationship;
    }

    @Benchmark
    public void _01_benchmark_cypher(Blackhole b) {

        db.execute("MATCH (n {id:$head}) WITH n CALL algo.deltaStepping(n, 'cost', $delta" +
                ", {concurrency:$concurrency, write:false})" +
                " YIELD loadDuration, evalDuration, writeDuration RETURN " +
                "loadDuration, evalDuration, writeDuration", params)
                .accept(row -> {
                    b.consume(row);
                    return true;
                });

    }

    @Benchmark
    public void _02_benchmark_loadGraph(Blackhole b) {

        Graph graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("cost", Double.POSITIVE_INFINITY)
                .load(LightGraphFactory.class);

        b.consume(graph);
    }

    @Benchmark
    public void _03_benchmark_direct(Blackhole b) {

        Graph graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("cost", Double.POSITIVE_INFINITY)
                .load(LightGraphFactory.class);

        b.consume(new ShortestPathDeltaStepping(graph, delta)
                .withExecutorService(Pools.DEFAULT)
                .withConcurrency(concurrency)
                .compute(lines.get(0).getId())
                .getShortestPaths());
    }

}
