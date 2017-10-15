// tag::create-sample-graph[]

CREATE (a:Place{id:"A"}),
       (b:Place{id:"B"}),
       (c:Place{id:"C"}),
       (d:Place{id:"D"}),
       (e:Place{id:"E"})
CREATE (d)-[:LINK{cost:4}]->(b),
       (d)-[:LINK{cost:6}]->(e),
       (b)-[:LINK{cost:1}]->(a),
       (b)-[:LINK{cost:3}]->(c),
       (a)-[:LINK{cost:2}]->(c),
       (c)-[:LINK{cost:5}]->(e);

// end::create-sample-graph[]

// tag::write-sample-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.mst(n, 'cost', {write:true, writeProperty:"mst"})
YIELD loadMillis, computeMillis, writeMillis, weightSum, weightMin,weightMax, relationshipCount
RETURN loadMillis,computeMillis,writeMillis,weightSum,weightMin,weightMax,relationshipCount;

// tag::write-sample-graph[]