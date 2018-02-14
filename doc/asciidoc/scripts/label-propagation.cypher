// tag::create-sample-graph[]

CREATE (nAlice:User {id:'Alice',predefined_label:52})
,(nBridget:User {id:'Bridget',predefined_label:21})
,(nCharles:User {id:'Charles',predefined_label:43})
,(nDoug:User {id:'Doug',predefined_label:21})
,(nMark:User {id:'Mark',predefined_label: 19})
,(nMichael:User {id:'Michael', predefined_label: 52})
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

CALL algo.labelPropagation('User', 'FOLLOW','OUTGOING',
  {iterations:10,partitionProperty:'partition', write:true})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, partitionProperty;

// end::write-sample-graph[]


// tag::write-existing-label-sample-graph[]

CALL algo.labelPropagation('User', 'FOLLOW','OUTGOING',
  {iterations:10,partitionProperty:'predefined_label', write:true})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, partitionProperty;

// end::write-existing-label-sample-graph[]

// tag::cypher-loading[]

CALL algo.labelPropagation(
  'MATCH (p:User) RETURN id(p) as id, p.weight as weight, id(p) as value',
  'MATCH (p1:User)-[f:FRIEND]->(p2:User)
   RETURN id(p1) as source, id(p2) as target, f.weight as weight',
  "OUT",
  {graph:'cypher',write:true});

// end::cypher-loading[]



