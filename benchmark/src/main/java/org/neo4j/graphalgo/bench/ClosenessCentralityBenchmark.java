package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.ClosenessCentralityProc;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
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
public class ClosenessCentralityBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    @Param({"LIGHT", "HEAVY", "HUGE"})
    public GraphImpl graph;

    @Param({"30"})
    public int netSize;

    private GraphDatabaseAPI db;

    private Map<String, Object> params;

    @Setup
    public void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ClosenessCentralityProc.class);

        try (ProgressTimer ignored = ProgressTimer.start(l -> System.out.println("setup took " + l + "ms"))) {
            createNet(netSize); // size^2 nodes; size^3 edges
        }

        params = MapUtil.map("graph", graph.name());
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
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
                            temp.get(j).createRelationshipTo(line.get(k), RELATIONSHIP_TYPE);
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
        return db.execute("CALL algo.closeness('','', {write:false, stats:false, graph: $graph}) YIELD " +
                "nodes, loadMillis, computeMillis, writeMillis", params)
                .stream()
                .count();
    }

}
