// tag::create-sample-graph[]

CREATE (a:Loc{name:'A'}), (b:Loc{name:'B'}), (c:Loc{name:'C'}), 
       (d:Loc{name:'D'}), (e:Loc{name:'E'}), (f:Loc{name:'F'}),
       (a)-[:ROAD {cost:50}]->(b),
       (a)-[:ROAD {cost:50}]->(c),
       (a)-[:ROAD {cost:100}]->(d),
       (a)-[:RAIL {cost:50}]->(d),
       (b)-[:ROAD {cost:40}]->(d),
       (c)-[:ROAD {cost:40}]->(d),
       (c)-[:ROAD {cost:80}]->(e),
       (d)-[:ROAD {cost:30}]->(e),
       (d)-[:ROAD {cost:80}]->(f),
       (e)-[:ROAD {cost:40}]->(f),
       (e)-[:RAIL {cost:20}]->(f);

// end::create-sample-graph[]


// tag::single-pair-stream-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath.stream(start, end, 'cost') 
YIELD nodeId, cost
RETURN nodeId, cost LIMIT 20

// end::single-pair-stream-sample-graph[]


// tag::single-pair-write-sample-graph[]

MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})
CALL algo.shortestPath(start, end, 'cost',{write:true,writeProperty:'sssp'}) 
YIELD writeMillis,loadMillis,nodeCount, totalCost
RETURN writeMillis,loadMillis,nodeCount,totalCost

// end::single-pair-write-sample-graph[]


// tag::delta-stream-sample-graph[]

MATCH (n:Loc {name:'A'})
CALL algo.shortestPath.deltaStepping.stream(n, 'cost', 3.0)
YIELD nodeId, distance 
RETURN nodeId, distance LIMIT 20

// end::delta-stream-sample-graph[]


// tag::delta-write-sample-graph[]

MATCH (n:Loc {name:'A'})
CALL algo.shortestPath.deltaStepping(n, 'cost', 3.0, {defaultValue:1.0, write:true, writeProperty:'sssp'})
YIELD nodeCount, loadDuration, evalDuration, writeDuration 
RETURN nodeCount, loadDuration, evalDuration, writeDuration

// end::delta-write-sample-graph[]


// tag::all-pairs-sample-graph[]

CALL algo.allShortestPaths.stream('cost',{nodeQuery:'Loc',defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance
WITH sourceNodeId, targetNodeId, distance 
WHERE algo.isFinite(distance) = true
RETURN sourceNodeId, targetNodeId, distance LIMIT 20

// end::all-pairs-sample-graph[]


// tag::all-pairs-bidirected-graph[]

CALL algo.allShortestPaths.stream('cost', {
nodeQuery:'MATCH (n:Loc) RETURN id(n) as id', 
relationshipQuery:'MATCH (n:Loc)-[r]-(p:Loc) RETURN id(n) as source, id(p) as target, r.cost as weight',
graph:'cypher', defaultValue:1.0})


// end::all-pairs-bidirected-graph[]
