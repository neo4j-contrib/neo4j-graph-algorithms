package algo.bench;

import algo.algo.WeaklyConnectedComponents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConnectedComponentsBenchmark {

  private static GraphDatabaseService db;

  @Setup
  public void setup() throws KernelException {
    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    Procedures proceduresService = ((GraphDatabaseAPI) db)
        .getDependencyResolver()
        .resolveDependency(Procedures.class);
    proceduresService.registerProcedure(WeaklyConnectedComponents.class);
    proceduresService.registerFunction(WeaklyConnectedComponents.class);

    String CC_GRAPH =
        "CREATE (a:Node {name:'A'})" +
            "CREATE (b:Node {name:'B'})" +
            "CREATE (c:Node {name:'C'})-[:LINK]->(d:Node {name:'D'})-[:LINK]->(e:Node {name:'E'})-[:LINK]->(c)  " +
            "CREATE (f:Node {name:'F'})-[:LINK]->(g:Node {name:'G'})-[:LINK]->(f)  " +
            "CREATE (o:Node {name:'O'})" +
            "CREATE (h:Node {name:'H'})-[:LINK]->(o)-[:LINK]->(h)  " +
            "CREATE (i:Node {name:'I'})-[:LINK]->(o)  " +
            "CREATE (j:Node {name:'J'})-[:LINK]->(o)  " +
            "CREATE (k:Node {name:'K'})-[:LINK]->(o)  " +
            "CREATE (l:Node {name:'L'})-[:LINK]->(o)  " +
            "CREATE (m:Node {name:'M'})-[:LINK]->(o)  " +
            "CREATE (n:Node {name:'N'})-[:LINK]->(o)";
    db.execute(CC_GRAPH).close();
  }

  @TearDown
  public void teardown() {
    db.shutdown();
  }

  @Benchmark
  public int wccCount() {
    final String query = "CALL algo.algo.wcc()";
    int count = 0;
    try (Transaction tx = db.beginTx()) {
      final Result result = db.execute(query, Collections.emptyMap());
      while (result.hasNext()) {
        result.next();
        count++;
      }
      tx.success();
    }
    return count;
  }


}
