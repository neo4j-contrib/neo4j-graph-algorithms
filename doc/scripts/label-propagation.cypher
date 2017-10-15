// tag::create-sample-graph[]

CREATE (nAlice:User {id:'Alice'})
,(nBridget:User {id:'Bridget'})
,(nCharles:User {id:'Charles'})
,(nDoug:User {id:'Doug'})
,(nMark:User {id:'Mark'})
,(nMichael:User {id:'Michael'})
CREATE (nAlice)-[:FOLLOW]->(nBridget)
,(nAlice)-[:FOLLOW]->(nCharles)
,(nMark)-[:FOLLOW]->(nDoug)
,(nBridget)-[:FOLLOW]->(nMichael)
,(nDoug)-[:FOLLOW]->(nMark)
,(nMichael)-[:FOLLOW]->(nAlice)
,(nAlice)-[:FOLLOW]->(nMichael)
,(nBridget)-[:FOLLOW]->(nAlice)
,(nMichael)-[:FOLLOW]->(nBridget)
,(nCharles)-[:FOLLOW]->(nDoug);

// end::create-sample-graph[]

// tag::write-sample-graph[]

CALL algo.labelPropagation('User', 'FOLLOW','OUTGOING', {iterations:10,partitionProperty:'partition', write:true}) 
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, partitionProperty;

// tag::write-sample-graph[]