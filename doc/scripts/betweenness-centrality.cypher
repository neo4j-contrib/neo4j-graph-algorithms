// tag::create-sample-graph[]

CREATE (nAlice:User {id:'Alice'})
,(nBridget:User {id:'Bridget'})
,(nCharles:User {id:'Charles'})
,(nDoug:User {id:'Doug'})
,(nMark:User {id:'Mark'})
,(nMichael:User {id:'Michael'})
CREATE (nAlice)-[:MANAGE]->(nBridget)
,(nAlice)-[:MANAGE]->(nCharles)
,(nAlice)-[:MANAGE]->(nDoug)
,(nMark)-[:MANAGE]->(nAlice)
,(nCharles)-[:MANAGE]->(nMichael);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.betweenness.stream('User','MANAGE',{direction:'out'}) 
YIELD nodeId, centrality 
RETURN nodeId,centrality order by centrality desc limit 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.betweenness('User','MANAGE', {direction:'out',write:true, writeProperty:'centrality'}) 
YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.betweenness(
'MATCH (p:User) RETURN id(p) as id',
'MATCH (p1:User)-[:MANAGE]->(p2:User) RETURN id(p1) as source, id(p2) as target',
{graph:'cypher', write: true});

// end::cypher-loading[]

// tag::stream-rabrandes-graph[]

CALL algo.betweenness.sampled.stream('User','FRIEND', 
{strategy:'random', probability:1.0, maxDepth:5}) 
YIELD nodeId, centrality

// end::stream-rabrandes-graph[]

// tag::write-rabrandes-graph[]

CALL algo.betweenness.sampled('User','FRIEND', 
{strategy:'random', probability:1.0, writeProperty:'centrality', maxDepth:5}) 
YIELD nodes, minCentrality, maxCentrality

// end::write-rabrandes-graph[]

