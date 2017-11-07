package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class UnionFindsTest {

    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static final int setsCount = 16;
    private static final int setSize = 10;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{LightGraphFactory.class, "Light"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "Kernel"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println(
                "creating test graph took " + l + " ms"))) {
            int[] setSizes = new int[setsCount];
            Arrays.fill(setSizes, setSize);
            createTestGraph(setSizes);
        }
    }

    private Graph graph;

    public UnionFindsTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        graph = new GraphLoader(DB)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(graphImpl);
    }

    private static void createTestGraph(int... setSizes) {
        DB.executeAndCommit(db -> {
            for (int setSize : setSizes) {
                createLine(db, setSize);
            }
        });
    }

    private static void createLine(GraphDatabaseService db, int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();
            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }
    }

    @Test
    public void testSeq() {
        test(UnionFindAlgo.SEQ);
    }

    @Test
    public void testQueue() {
        test(UnionFindAlgo.QUEUE);
    }

    @Test
    public void testForkJoin() {
        test(UnionFindAlgo.FORK_JOIN);
    }

    @Test
    public void testFJMerge() {
        test(UnionFindAlgo.FJ_MERGE);
    }


    private void test(UnionFindAlgo uf) {
        DSSResult result = run(uf);

        assertEquals(setsCount, result.getSetCount());
        int[] setRegions = new int[setsCount];
        Arrays.fill(setRegions, -1);

        result.forEach(graph, (nodeId, setId) -> {
            int expectedSetRegion = nodeId / setSize;
            int setRegion = setId / setSize;
            assertEquals(
                    "Node " + nodeId + " in unexpected set: " + setId,
                    expectedSetRegion,
                    setRegion);

            int regionSetId = setRegions[setRegion];
            if (regionSetId == -1) {
                setRegions[setRegion] = setId;
            } else {
                assertEquals(
                        "Inconsistent set for node " + nodeId + ", is " + setId + " but should be " + regionSetId,
                        regionSetId,
                        setId);
            }
            return true;
        });
    }

    private DSSResult run(final UnionFindAlgo uf) {
        return uf.runAny(
                graph,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                setSize / Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT_CONCURRENCY,
                Double.NaN,
                UnionFindAlgo.NOTHING);
    }
}
