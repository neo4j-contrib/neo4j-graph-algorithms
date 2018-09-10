/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.algo;

import org.junit.*;
import org.neo4j.graphalgo.JaccardProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.walking.NodeWalkerProc;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class JaccardTest {

    private static GraphDatabaseAPI db;
    private Transaction tx;

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(JaccardProc.class);
        db.execute(buildDatabaseQuery()).close();
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

    private static void buildRandomDB(int size) {
        db.execute("MATCH (n) DETACH DELETE n").close();
        db.execute("UNWIND range(1,$size/10) as _ CREATE (:Person) CREATE (:Item) ",singletonMap("size",size)).close();
        String statement =
                "MATCH (p:Person) WITH collect(p) as people " +
                "MATCH (i:Item) WITH people, collect(i) as items " +
                "UNWIND range(1,$size) as _ " +
                "WITH people[toInteger(rand()*size(people))] as p, items[toInteger(rand()*size(items))] as i " +
                "MERGE (p)-[:LIKES]->(i) RETURN count(*) ";
        db.execute(statement,singletonMap("size",size)).close();
    }
    private static String buildDatabaseQuery() {
        return "CREATE (a:Person {name:'Alice'})\n" +
                "CREATE (b:Person {name:'Bob'})\n" +
                "CREATE (c:Person {name:'Charlie'})\n" +
                "CREATE (i1:Item {name:'p1'})\n" +
                "CREATE (i2:Item {name:'p2'})\n" +
                "CREATE (i3:Item {name:'p3'})\n" +

                "CREATE" +
                " (a)-[:LIKES]->(i1),\n" +
                " (a)-[:LIKES]->(i2),\n" +
                " (a)-[:LIKES]->(i3),\n" +
                " (b)-[:LIKES]->(i1),\n" +
                " (b)-[:LIKES]->(i2),\n" +
                " (c)-[:LIKES]->(i3)\n";
        // a: 3
        // b: 2
        // c: 1
        // a / b = 2 : 2/3
        // a / c = 1 : 1/3
        // b / c = 0 : 0/3 = 0
        //
    }


    @Test
    public void jaccardSingleMultiThreadComparision() {
        int size = 3333;
        buildRandomDB(size);
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "call algo.similarity.jaccard.stream(data,{similarityCutoff:-0.1,concurrency:$threads}) " +
                "yield source1, source2, count1, count2, intersection, similarity " +
                "RETURN source1, source2, count1, count2, intersection, similarity ORDER BY source1,source2";
        Result result1 = db.execute(query, singletonMap("threads", 1));
        Result result2 = db.execute(query, singletonMap("threads", 2));
        Result result4 = db.execute(query, singletonMap("threads", 4));
        Result result8 = db.execute(query, singletonMap("threads", 8));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        int people = size/10;
        assertEquals((people * people - people)/2,count);
    }

    @Test
    public void jaccardSingleMultiThreadComparisionTopK() {
        int size = 3333;
        buildRandomDB(size);
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "call algo.similarity.jaccard.stream(data,{similarityCutoff:-0.1,concurrency:$threads,topK:1}) " +
                "yield source1, source2, count1, count2, intersection, similarity " +
                "RETURN source1, source2, count1, count2, intersection, similarity ORDER BY source1,source2";
        Result result1 = db.execute(query, singletonMap("threads", 1));
        Result result2 = db.execute(query, singletonMap("threads", 2));
        Result result4 = db.execute(query, singletonMap("threads", 4));
        Result result8 = db.execute(query, singletonMap("threads", 8));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        int people = size/10;
        assertEquals(people,count);
    }

    @Test
    public void topNJaccardStreamTest() {
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "call algo.similarity.jaccard.stream(data,{top:2}) " +
                "yield source1, source2, count1, count2, intersection, similarity " +
                "RETURN * ORDER BY source1,source2";


        Result results = db.execute(query);
        int count = 0;
        assertTrue(results.hasNext());
        Map<String, Object> row = results.next();
        assertEquals(0L, row.get("source1"));
        assertEquals(1L, row.get("source2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        assertEquals(2L, row.get("intersection"));
        assertEquals(2.0D / 3, row.get("similarity"));
        count++;
        assertTrue(results.hasNext());
        row = results.next();
        assertEquals(0L, row.get("source1"));
        assertEquals(2L, row.get("source2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        assertEquals(1L, row.get("intersection"));
        assertEquals(1.0D / 3, row.get("similarity"));
        count++;
        assertFalse(results.hasNext());
        assertEquals(2, count);
    }

    @Test
    public void jaccardStreamTest() {
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "call algo.similarity.jaccard.stream(data) " +
                "yield source1, source2, count1, count2, intersection, similarity " +
                "RETURN * ORDER BY source1,source2";


        Result results = db.execute(query);
        int count = 0;
        assertTrue(results.hasNext());
        Map<String, Object> row = results.next();
        assertEquals(0L, row.get("source1"));
        assertEquals(1L, row.get("source2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        assertEquals(2L, row.get("intersection"));
        assertEquals(2.0D / 3, row.get("similarity"));
        count++;
        assertTrue(results.hasNext());
        row = results.next();
        assertEquals(0L, row.get("source1"));
        assertEquals(2L, row.get("source2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        assertEquals(1L, row.get("intersection"));
        assertEquals(1.0D / 3, row.get("similarity"));
        count++;
        row = results.next();
        assertEquals(1L, row.get("source1"));
        assertEquals(2L, row.get("source2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0D / 3, row.get("similarity"));
        count++;
        assertFalse(results.hasNext());
        assertEquals(3, count);
    }

    @Test
    public void topKJaccardStreamTest() {
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "call algo.similarity.jaccard.stream(data,{topK:1, concurrency:1}) " +
                "yield source1, source2, count1, count2, intersection, similarity " +
                "RETURN * ORDER BY source1,source2";

        System.out.println(db.execute(query).resultAsString());

        Result results = db.execute(query);
        int count = 0;
        assertTrue(results.hasNext());
        Map<String, Object> row = results.next();
        assertEquals(0L, row.get("source1"));
        assertEquals(1L, row.get("source2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        assertEquals(2L, row.get("intersection"));
        assertEquals(2.0D / 3, row.get("similarity"));
        count++;
        row = results.next();
        assertEquals(1L, row.get("source1"));
        assertEquals(0L, row.get("source2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(2L, row.get("intersection"));
        assertEquals(2.0D / 3, row.get("similarity"));
        count++;
        assertTrue(results.hasNext());
        row = results.next();
        assertEquals(2L, row.get("source1"));
        assertEquals(0L, row.get("source2"));
        assertEquals(1L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(1L, row.get("intersection"));
        assertEquals(1D / 3, row.get("similarity"));
        count++;
        assertFalse(results.hasNext());
        assertEquals(3, count);
    }

    @Test
    public void simpleJaccardTest() {
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "CALL algo.similarity.jaccard(data, {similarityCutoff: 0.0}) " +
                "yield p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs " +
                "RETURN *";


        Result results = db.execute(query);
        Map<String, Object> row = results.next();

        assertEquals((double) row.get("p50"), 0.33, 0.01);
        assertEquals((double) row.get("p95"), 0.66, 0.01);
        assertEquals((double) row.get("p99"), 0.66, 0.01);
        assertEquals((double) row.get("p100"), 0.66, 0.01);
    }

    @Test
    public void simpleJaccardWriteTest() {
        String query = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
                "WITH {source:id(p), targets: collect(distinct id(i))} as userData\n" +
                "WITH collect(userData) as data\n" +
                "CALL algo.similarity.jaccard(data, {similarityCutoff: 0.1, write: true}) " +
                "yield p50, p75, p90, p99, p999, p100, nodes, similarityPairs " +
                "RETURN *";


        db.execute(query);

        String checkSimilaritiesQuery = "MATCH (a)-[similar:SIMILAR]-(b)" +
                "RETURN a.name AS node1, b.name as node2, similar.score AS score " +
                "ORDER BY id(a), id(b)";

        Result result = db.execute(checkSimilaritiesQuery);

        assertTrue(result.hasNext());
        Map<String, Object> row = result.next();
        assertEquals(row.get("node1"), "Alice");
        assertEquals(row.get("node2"), "Bob");
        assertEquals((double) row.get("score"), 0.66, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Alice");
        assertEquals(row.get("node2"), "Charlie");
        assertEquals((double) row.get("score"), 0.33, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Bob");
        assertEquals(row.get("node2"), "Alice");
        assertEquals((double) row.get("score"), 0.66, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Charlie");
        assertEquals(row.get("node2"), "Alice");
        assertEquals((double) row.get("score"), 0.33, 0.01);

    }

}
