package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Node;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ShortestPathBenchmark {

    private static GraphDatabaseAPI db;
    private static List<Node> lines = new ArrayList<>();
    private static long headId;

    @Param({"10", "20", "50", "100", "500"})
    static int size;

    @Setup
    public static void setup() throws KernelException {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathDeltaSteppingProc.class);

        createGrid(size);

        headId = lines.get(0).getId();


    }

    private static void createGrid(int size) {
        try (Transaction tx = db.beginTx()) {
            List<Node> temp = null;
            for (int i = 0; i < size; i++) {
                List<Node> line = createLine(size);
                if (null != temp) {
                    for (int j = 0; j < size; j++) {
                        temp.get(j).createRelationshipTo(line.get(j), RelationshipType.withName("TYPE"));
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
            temp.createRelationshipTo(node, RelationshipType.withName("TYPE"));
            temp = node;
        }
        return nodes;
    }

    @Benchmark
    public Object _01_benchmark() {
        return db.execute("MATCH (n {id:'" + headId + "'}) WITH n CALL algo.deltaStepping.stream(n, 'cost', 1.5)" +
                " YIELD nodeId, distance RETURN nodeId, distance").stream();
    }

}
