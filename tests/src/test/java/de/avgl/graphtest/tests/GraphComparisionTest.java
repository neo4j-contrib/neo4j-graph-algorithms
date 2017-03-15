package de.avgl.graphtest.tests;

import algo.algo.PageRank;
import org.junit.*;
import org.neo4j.graphalgo.impl.TestProcedure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mknobloch
 */
@Ignore("depends on local data")
public class GraphComparisionTest {

    private static final Logger LOGGER = getLogger(GraphComparisionTest.class);

    private GraphDatabaseService db;

    private static final Path GRAPH_PATH = Paths.get("/Users/mknobloch/Documents/graph/");


    public void registerProcedure(Class<?>...procedures) throws KernelException {
        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        for (Class<?> procedure : procedures) {
            proceduresService.registerProcedure(procedure);
            proceduresService.registerFunction(procedure);
        }
    }

    @Before
    public void setup() throws KernelException {
        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(GRAPH_PATH.toFile())
                .newGraphDatabase();

        registerProcedure(TestProcedure.class);
        registerProcedure(PageRank.class);

    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Ignore("takes very long")
    @Test
    public void testView() throws Exception {
        final Result result = db.execute("CALL graphalgo.pagerank.view({label:'Node', iterations:1, dampingFactor:0.85})");
    }

    @Test
    public void testHeavy() throws Exception {
        final Result result = db.execute("CALL graphalgo.pagerank.heavy({iterations:20, dampingFactor:0.85})");
        assertEquals(3697509, result.stream().count());
    }

    @Test
    public void testLight() throws Exception {
        final Result result = db.execute("CALL graphalgo.pagerank.light({iterations:20, dampingFactor:0.85})");
        assertEquals(3697509, result.stream().count());
    }

    @Test
    public void testAlgo() throws Exception {
        final Result result = db.execute("CALL algo.algo.pageRankWithCypher({iterations:20, write:false})");
        assertTrue(result.hasNext());
    }

}
