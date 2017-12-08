package org.neo4j.graphalgo.core.huge;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIntersect;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public final class HugeIntersectionTest {

    private static final int DEGREE = 25;
    private static HugeGraph GRAPH;
    private static HugeRelationshipIntersect INTERSECT;
    private static long START1;
    private static long START2;
    private static long START3;
    private static long[] ALL_TARGETS;
    private static long[] SOME_TARGETS;

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() {
        long[] neoStarts = new long[3];
        long[] neoTargets = DB.executeAndCommit(db -> {
            try (Statement st = DB.statement()) {
                TokenWriteOperations token = st.tokenWriteOperations();
                int type = token.relationshipTypeGetOrCreateForName("TYPE");
                DataWriteOperations write = st.dataWriteOperations();
                final Random random = new Random();
                final long start1 = write.nodeCreate();
                final long start2 = write.nodeCreate();
                final long start3 = write.nodeCreate();
                neoStarts[0] = start1;
                neoStarts[1] = start2;
                neoStarts[2] = start3;
                final long[] targets = new long[DEGREE * 2];
                int some = DEGREE;
                for (int i = 0; i < DEGREE; i++) {
                    targets[i] = write.nodeCreate();
                    write.relationshipCreate(type, start1, targets[i]);
                    if (random.nextBoolean()) {
                        write.relationshipCreate(type, start2, targets[i]);
                        targets[some++] = targets[i];
                    }
                }
                return Arrays.copyOf(targets, some);
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
        });

        GRAPH = (HugeGraph) new GraphLoader(DB).asUndirected(true).load(HugeGraphFactory.class);
        INTERSECT = GRAPH.intersectionCopy();
        START1 = GRAPH.toHugeMappedNodeId(neoStarts[0]);
        START2 = GRAPH.toHugeMappedNodeId(neoStarts[1]);
        START3 = GRAPH.toHugeMappedNodeId(neoStarts[2]);
        ALL_TARGETS = new long[DEGREE];
        SOME_TARGETS = new long[neoTargets.length - DEGREE];
        Arrays.setAll(ALL_TARGETS, i -> GRAPH.toHugeMappedNodeId(neoTargets[i]));
        Arrays.setAll(SOME_TARGETS, i -> GRAPH.toHugeMappedNodeId(neoTargets[i + DEGREE]));
        Arrays.sort(ALL_TARGETS);
        Arrays.sort(SOME_TARGETS);
    }

    @Test
    public void intersectWithCompleteTargets() {
        final long[] intersect = INTERSECT.intersect(START1, START1);
        assertArrayEquals(ALL_TARGETS, intersect);
    }

    @Test
    public void intersectWithEmptyTargets() {
        final long[] intersect = INTERSECT.intersect(START1, START3);
        assertArrayEquals(new long[0], intersect);
    }

    @Test
    public void intersectWithSomeExistingTargets() {
        final long[] intersect = INTERSECT.intersect(START1, START2);
        assertArrayEquals(SOME_TARGETS, intersect);
    }
}
