// tag::create-sample-graph[]

CREATE (a:Node{id:"A"}),
       (b:Node{id:"B"}),
       (c:Node{id:"C"}),
       (d:Node{id:"D"}),
       (e:Node{id:"E"})
CREATE (a)-[:LINK]->(b),
       (b)-[:LINK]->(c),
       (d)-[:LINK]->(e);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.harmonic.stream('Node', 'LINKS') YIELD nodeId, centrality
RETURN nodeId,centrality order by centrality desc limit 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.harmonic('Node', 'LINK', {writeProperty:'centrality'}) 
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.harmonic(
  'MATCH (p:Node) RETURN id(p) as id',
  'MATCH (p1:Node)-[:LINK]-(p2:Node) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', writeProperty: 'centrality'}
);

// end::cypher-loading[]