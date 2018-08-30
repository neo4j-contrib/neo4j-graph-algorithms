package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongHashSet;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 26.08.18
 */
public class IntersectionTest {

    private long[][][] data = {
            {{1,2,4},{1,3,5},{1}},
            {{1,2,4},{1,2,5},{2}},
            {{1,2,4},{1,2,4},{3}},
            {{1,2,4},{1,2,4,5},{3}},
            {{1,2,4,5},{1,4,5},{3}},
            {{},{},{0}},
            {{},{1},{0}},
            {{1},{},{0}},
            {{1},{1},{1}},
            {{1},{0},{0}},
            {{0},{1},{0}},
            {{1,2,4,5},{1,5},{2}},
            {{1,2,4,5},{1,2},{2}},
            {{1,2,4,5},{1,4},{2}},
            {{1,2,4,5},{2,4},{2}},
            {{1,2,4,5},{2,5},{2}},
            {{1,2,4},{0,3,5},{0}}
    };
    @Test
    public void intersection() throws Exception {
        for (long[][] row : data) {
            assertEquals(row[2][0], JaccardProc.intersection(LongHashSet.from(row[0]),LongHashSet.from(row[1])));
        }
    }

    @Test
    public void intersection2() throws Exception {
        for (long[][] row : data) {
            assertEquals(row[2][0], JaccardProc.intersection2(row[0],row[1]));
        }

    }

    @Test
    public void intersection3() throws Exception {
        for (long[][] row : data) {
            System.out.println(Arrays.deepToString(row));
            assertEquals(Arrays.toString(row),row[2][0], JaccardProc.intersection3(row[0],row[1]));
        }
    }
    @Test
    public void intersection4() throws Exception {
        for (long[][] row : data) {
            System.out.println(Arrays.deepToString(row));
            assertEquals(Arrays.toString(row),row[2][0], JaccardProc.intersection4(row[0],row[1]));
        }
    }

}
