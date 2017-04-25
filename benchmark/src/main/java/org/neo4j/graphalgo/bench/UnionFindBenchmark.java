package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.sources.BufferedAllRelationshipIterator;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.graphalgo.impl.UnionFind;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class UnionFindBenchmark {

    private GraphDatabaseAPI db;
    private SingleRunAllRelationIterator singleRunAllRelationIterator;
    private LazyIdMapper idMapping;
    private BufferedAllRelationshipIterator iterator;
    private Graph heavyGraph;
    private Graph lightGraph;

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
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +
                "  (nD)-[:TYPE]->(nA),\n" +

                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +
                "  (nG)-[:TYPE]->(nH),\n" +
                "  (nH)-[:TYPE]->(nI),\n" +
                "  (nI)-[:TYPE]->(nE),\n" +

                "  (nJ)-[:TYPE]->(nK),\n" +
                "  (nK)-[:TYPE]->(nJ);";

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        idMapping = LazyIdMapper.importer(db)
                .withAnyLabel()
                .build();

        iterator = BufferedAllRelationshipIterator.importer(db)
                .withIdMapping(idMapping)
                .withAnyLabel()
                .withAnyRelationshipType()
                .build();

        singleRunAllRelationIterator = new SingleRunAllRelationIterator(db, idMapping);

        heavyGraph = loadHeavy();

        lightGraph = loadLight();
    }

    @Benchmark
    public Object _01_unbufferedUnionFind() {
        return new UnionFind(idMapping, singleRunAllRelationIterator)
                .compute()
                .getSetSize();
    }

    @Benchmark
    public Object _02_bufferedUnionFind() {
        return new UnionFind(idMapping, iterator)
                .compute()
                .getSetSize();
    }

    @Benchmark
    public Object _03_heavyGraphUnionFind() {
        return new GraphUnionFind(heavyGraph)
                .compute()
                .getSetSize();
    }

    @Benchmark
    public Object _04_lightGraphUnionFind() {
        return new GraphUnionFind(lightGraph)
                .compute()
                .getSetSize();
    }

    @Benchmark
    public Object _05_buildBufferedDataSource() {

        final LazyIdMapper lazyIdMapper = LazyIdMapper.importer(db)
                .withAnyLabel()
                .build();

        return BufferedAllRelationshipIterator.importer(db)
                .withIdMapping(lazyIdMapper)
                .withAnyLabel()
                .withAnyRelationshipType()
                .build();
    }

    @Benchmark
    public Object _06_buildHeavyGraph() {
        return loadHeavy();
    }

    @Benchmark
    public Object _07_buildLightGraph() {
        return loadLight();
    }

    private Graph loadLight() {
        return new GraphLoader(db).load(LightGraphFactory.class);
    }

    private Graph loadHeavy() {
        return new GraphLoader(db).load(HeavyGraphFactory.class);
    }
}
