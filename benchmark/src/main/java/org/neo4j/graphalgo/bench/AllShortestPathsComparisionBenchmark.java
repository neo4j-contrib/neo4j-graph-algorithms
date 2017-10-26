package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.ShortestPathsProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.AllShortestPaths;
import org.neo4j.graphalgo.impl.HugeMSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.MSBFSAllShortestPaths;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

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
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AllShortestPathsComparisionBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private GraphDatabaseAPI db;
    private List<Node> lines = new ArrayList<>();

    private final Map<String, Object> params = new HashMap<>();
    private Graph graph;

    @Setup
    public void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        createNet(42); // 1764 nodes; 74088 edges
        params.put("head", lines.get(0).getId());
        params.put("delta", 2.5);

//        graph = new GraphLoader(db).withRelationshipWeightsFromProperty("cost", 1.0).load(HeavyGraphFactory.class);
        graph = new GraphLoader(db).withRelationshipWeightsFromProperty("cost", 1.0).load(HugeGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        graph.release();
        Pools.DEFAULT.shutdown();
    }

    private void createNet(int size) {
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
                            createRelation(temp.get(j), line.get(k));
                        }
                    }
                }
                temp = line;
            }
            tx.success();
        }
    }

    private List<Node> createLine(int length) {
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
        double rndCost = Math.random() * 5.0; //(to.getId() % 5) + 1.0; // (0-5)
        relationship.setProperty("cost", rndCost);
        return relationship;
    }

    @Benchmark
    public long _01_benchmark_ASP() {
        return new AllShortestPaths(graph, Pools.DEFAULT, 8)
                .resultStream()
                .count();
    }

    @Benchmark
    public long _02_benchmark_MS_ASP() {
        return new MSBFSAllShortestPaths(graph, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .resultStream().count();
    }

    @Benchmark
    public long _03_benchmark_Huge_MS_ASP() {
        return new HugeMSBFSAllShortestPaths((HugeGraph) graph, AllocationTracker.EMPTY, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .resultStream().count();
    }
}
