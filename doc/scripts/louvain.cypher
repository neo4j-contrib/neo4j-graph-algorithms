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

CALL algo.clustering.louvain.stream('User', 'FRIEND', 
{}) 
YIELD nodeId, setId
RETURN nodeId, setId LIMIT 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.clustering.louvain('User', 'FRIEND', 
{write:true, writeProperty:'community'}) 
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis; 

// end::write-sample-graph[]

// tag::write-yelp[]

CALL algo.clustering.louvain('Business', 'CO_OCCURENT_REVIEWS', 
{write:true, writeProperty:'community'}) 
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis; 

// tag::end-yelp[]

