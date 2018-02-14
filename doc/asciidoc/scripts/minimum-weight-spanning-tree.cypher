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

// tag::write-sample-minst-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.minimum('Place','LINK', 'cost',id(n), {write:true, writeProperty:"MINST"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;

// end::write-sample-minst-graph[]


// tag::write-sample-maxst-graph[]


MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.maximum('Place', 'LINK', 'cost',id(n), {write:true, writeProperty:"MAXST"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;

// end::write-sample-maxst-graph[]


// tag::write-sample-kmaxst-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.kmax('Place', 'LINK', 'cost',id(n), 3, {writeProperty:"kmaxst"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;

// end::write-sample-kmaxst-graph[]

// tag::write-sample-kminst-graph[]

MATCH (n:Place{id:"D"}) 
CALL algo.spanningTree.kmin('Place', 'LINK', 'cost',id(n), 3, {writeProperty:"kminst"})
YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount
RETURN loadMillis,computeMillis,writeMillis, effectiveNodeCount;


// end::write-sample-kminst-graph[]
