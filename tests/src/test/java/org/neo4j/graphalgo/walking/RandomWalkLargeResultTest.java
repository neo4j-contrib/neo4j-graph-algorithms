package org.neo4j.graphalgo.walking;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class RandomWalkLargeResultTest {

    private static final int NODE_COUNT = 20000;

    private static GraphDatabaseAPI db;
    private Transaction tx;

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(NodeWalkerProc.class);

        db.execute(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT)).close();
    }

    @AfterClass
    public static void AfterClass() {
        db.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        tx = db.beginTx();
    }

    @After
    public void tearDown() throws Exception {
        tx.close();
    }

    private static String buildDatabaseQuery() {
        return "UNWIND range(0,$count) as id " +
                "CREATE (n:Node) " +
                "WITH collect(n) as nodes " +
                "unwind nodes as n with n, nodes[toInteger(rand()*10000)] as m " +
                "create (n)-[:FOO]->(m)";
    }

    @Test
    public void shouldHandleLargeResults() {
        Result results = db.execute("CALL algo.randomWalk.stream(null,null, null, 100, 100000)");

        assertEquals(100000,Iterators.count(results));
    }
}
