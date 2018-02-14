// tag::create-sample-graph[]

CREATE (a:Node{id:"A"}),
       (b:Node{id:"B"}),
       (c:Node{id:"C"}),
       (d:Node{id:"D"}),
       (e:Node{id:"E"})
CREATE (a)-[:LINK]->(b),
       (b)-[:LINK]->(a),
       (b)-[:LINK]->(c),
       (c)-[:LINK]->(b),
       (c)-[:LINK]->(d),
       (d)-[:LINK]->(c),
       (d)-[:LINK]->(e),
       (e)-[:LINK]->(d);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.closeness.stream('Node', 'LINKS') YIELD nodeId, centrality
RETURN nodeId,centrality order by centrality desc limit 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.closeness('Node', 'LINK', {write:true, writeProperty:'centrality'}) 
YIELD nodes,loadMillis, computeMillis, writeMillis;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.closeness(
  'MATCH (p:Node) RETURN id(p) as id',
  'MATCH (p1:Node)-[:LINK]->(p2:Node) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', write: true}
);

// end::cypher-loading[]