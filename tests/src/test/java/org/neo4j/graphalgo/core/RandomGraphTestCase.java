package org.neo4j.graphalgo.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.PrintWriter;

public abstract class RandomGraphTestCase {
    private static boolean hasFailures = false;

    protected static GraphDatabaseAPI db;

    public static final int NODE_COUNT = 100;

    @Rule
    public TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(
                final Throwable e,
                final Description description) {
            hasFailures = true;
        }
    };

    private static final String RANDOM_GRAPH =
            "FOREACH (x IN range(1, " + NODE_COUNT + ") | CREATE (:Label)) " +
                    "WITH 0.1 AS p " +
                    "MATCH (n1),(n2) WITH n1,n2 LIMIT 1000 WHERE rand() < p " +
                    "CREATE (n1)-[:TYPE {weight:ceil(10*rand())/10}]->(n2)";

    @BeforeClass
    public static void setupGraph() {
        db = buildGraph(RANDOM_GRAPH);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (hasFailures) {
            PrintWriter pw = new PrintWriter(System.out);
            pw.println("Generated graph to reproduce any errors:");
            pw.println();
            CypherExporter.export(pw, db);
        }
        db.shutdown();
    }

    protected static GraphDatabaseAPI buildGraph(String graph) {
        final GraphDatabaseService db =
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(graph).close();
            tx.success();
        } catch (Exception e) {
            markFailure();
            throw e;
        }
        return (GraphDatabaseAPI) db;
    }

    protected static void markFailure() {
        hasFailures = true;
    }
}
