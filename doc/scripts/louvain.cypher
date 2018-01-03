// tag::create-sample-graph[]

CREATE (nAlice:User {id:'Alice'})
,(nBridget:User {id:'Bridget'})
,(nCharles:User {id:'Charles'})
,(nDoug:User {id:'Doug'})
,(nMark:User {id:'Mark'})
,(nMichael:User {id:'Michael'})
CREATE (nAlice)-[:FRIEND]->(nBridget)
,(nAlice)-[:FRIEND]->(nCharles)
,(nMark)-[:FRIEND]->(nDoug)
,(nBridget)-[:FRIEND]->(nMichael)
,(nCharles)-[:FRIEND]->(nMark)
,(nAlice)-[:FRIEND]->(nMichael)
,(nCharles)-[:FRIEND]->(nDoug);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.louvain.stream('User', 'FRIEND', {})
YIELD nodeId, community
RETURN nodeId, community LIMIT 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.louvain('User', 'FRIEND',
  {write:true, writeProperty:'community'})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis; 

// end::write-sample-graph[]

// tag::write-yelp[]

CALL algo.louvain('Business', 'CO_OCCURENT_REVIEWS',
  {write:true, writeProperty:'community'})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis; 

// tag::end-yelp[]

// tag::cypher-loading[]

CALL algo.louvain(
  'MATCH (p:User) RETURN id(p) as id',
  'MATCH (p1:User)-[f:FRIEND]-(p2:User)
   RETURN id(p1) as source, id(p2) as target, f.weight as weight',
  {graph:'cypher',write:true});

// end::cypher-loading[]
