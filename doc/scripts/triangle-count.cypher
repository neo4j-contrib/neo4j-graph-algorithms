// tag::create-sample-graph[]

CREATE (alice:Person{id:"Alice"}),
       (michael:Person{id:"Michael"}),
       (karin:Person{id:"Karin"}),
       (chris:Person{id:"Chris"}),
       (will:Person{id:"Will"}),
       (mark:Person{id:"Mark"})
CREATE (michael)-[:KNOWS]->(karin),
       (michael)-[:KNOWS]->(chris),
       (will)-[:KNOWS]->(michael),
       (mark)-[:KNOWS]->(michael),
       (mark)-[:KNOWS]->(will),
       (alice)-[:KNOWS]->(michael),
       (will)-[:KNOWS]->(chris),
       (chris)-[:KNOWS]->(karin);

// end::create-sample-graph[]

// tag::stream-triples[]

CALL algo.triangle.stream('Person','KNOWS') 
yield nodeA,nodeB,nodeC;

// end::stream-triples[]


// tag::triangle-write-sample-graph[]

CALL algo.triangleCount('Person', 'KNOWS',
{concurrency:4, write:true, writeProperty:'triangles',clusteringCoefficientProperty:'coefficient'}) 
YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient;

// end::triangle-write-sample-graph[]

// tag::triangle-stream-sample-graph[]

CALL algo.triangleCount.stream('Person', 'KNOWS', {concurrency:4}) 
YIELD nodeId, triangles;

// end::triangle-stream-sample-graph[]

// tag::triangle-write-yelp[]

CALL algo.triangleCount('User', 'FRIEND',
{concurrency:4, write:true, writeProperty:'triangles',clusteringCoefficientProperty:'coefficient'}) 
YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount, averageClusteringCoefficient;

// end::triangle-write-yelp[]