package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.ShortestPathDeltaSteppingProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

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
    private static Graph graph;

    @Setup
    public static void setup() throws KernelException {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathDeltaSteppingProc.class);

        String createGraph = "CREATE (nA {name:'start'})\n" +
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
                "  (nB)-[:TYPE {cost:1.0}]->(nC),\n" +
                "  (nC)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nD)-[:TYPE {cost:3.0}]->(nA),\n" +
                "  (nD)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nE)-[:TYPE {cost:1.0}]->(nB),\n" +
                "  (nE)-[:TYPE {cost:2.0}]->(nD),\n" +
                "  (nE)-[:TYPE {cost:3.0}]->(nF),\n" +
                "  (nF)-[:TYPE {cost:4.0}]->(nB),\n" +
                "  (nF)-[:TYPE {cost:3.0}]->(nE),\n" +
                "  (nG)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nG)-[:TYPE {cost:1.0}]->(nE),\n" +
                "  (nH)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nH)-[:TYPE {cost:1.0}]->(nE),\n" +
                "  (nI)-[:TYPE {cost:2.0}]->(nB),\n" +
                "  (nI)-[:TYPE {cost:1.0}]->(nE),\n" +
                "  (nJ)-[:TYPE {cost:1.0}]->(nE),\n" +
                "  (nK)-[:TYPE {cost:3.0}]->(nE);";

        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

    }

    @Benchmark
    public Object _01_benchmark() {
        return db.execute("MATCH (n {name:'start'}) WITH n CALL algo.deltaStepping.stream(n, 'cost', 1.5)" +
                " YIELD nodeId, distance RETURN nodeId, distance").stream();
    }

}
