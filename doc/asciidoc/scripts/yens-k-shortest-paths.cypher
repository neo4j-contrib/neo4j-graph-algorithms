// tag::create-sample-graph[]

MERGE (a:Loc {name:'A'})
MERGE (b:Loc {name:'B'})
MERGE (c:Loc {name:'C'})
MERGE (d:Loc {name:'D'})
MERGE (e:Loc {name:'E'})
MERGE (f:Loc {name:'F'})

MERGE (a)-[:ROAD {cost:50}]->(b)
MERGE (a)-[:ROAD {cost:50}]->(c)
MERGE (a)-[:ROAD {cost:100}]->(d)
MERGE (b)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:40}]->(d)
MERGE (c)-[:ROAD {cost:80}]->(e)
MERGE (d)-[:ROAD {cost:30}]->(e)
MERGE (d)-[:ROAD {cost:80}]->(f)
MERGE (e)-[:ROAD {cost:40}]->(f);

// end::create-sample-graph[]


// tag::write-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.kShortestPaths(start, end, 3, 'cost' ,{}) 
YIELD resultCount
RETURN resultCount

// end::write-sample-graph[]


// tag::return-all-paths-sample-graph[]

MATCH p=()-[r:PATH_0|:PATH_1|:PATH_2]->() RETURN p LIMIT 25

// end::return-all-paths-sample-graph[]

// tag::cypher-loading[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.kShortestPaths(start, end, 3, 'cost',{
nodeQuery:'MATCH(n:Loc) WHERE not n.name = "C" RETURN id(n) as id',
relationshipQuery:'MATCH (n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) as source, id(m) as target, r.cost as weight',
graph:'cypher',writePropertyPrefix:'cypher_'}) 
YIELD resultCount
RETURN resultCount

// end::cypher-loading[]
