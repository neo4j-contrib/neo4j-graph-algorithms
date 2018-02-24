// tag::create-sample-graph[]

MERGE (nAlice:User {id:'Alice'})
MERGE (nBridget:User {id:'Bridget'})
MERGE (nCharles:User {id:'Charles'})
MERGE (nDoug:User {id:'Doug'})
MERGE (nMark:User {id:'Mark'})
MERGE (nMichael:User {id:'Michael'})

MERGE (nAlice)-[:FOLLOWS]->(nDoug)
MERGE (nAlice)-[:FOLLOWS]->(nBridget)
MERGE (nAlice)-[:FOLLOWS]->(nCharles)
MERGE (nMark)-[:FOLLOWS]->(nDoug)
MERGE (nMark)-[:FOLLOWS]->(nMichael)
MERGE (nBridget)-[:FOLLOWS]->(nDoug)
MERGE (nCharles)-[:FOLLOWS]->(nDoug)
MERGE (nMichael)-[:FOLLOWS]->(nDoug)

// end::create-sample-graph[]

// tag::stream-sample-graph[]

MATCH (u:User)
RETURN u.id AS name, SIZE((u)-[:FOLLOWS]->()) AS follows, SIZE((u)<-[:FOLLOWS]-()) AS followers

// end::stream-sample-graph[]
