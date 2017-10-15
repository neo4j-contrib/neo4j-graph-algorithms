// tag::create-sample-graph[]

CREATE (home:Page{name:'Home'})
,(about:Page{name:'About'})
,(product:Page{name:'Product'})
,(links:Page{name:'Links'})
,(a:Page{name:'Site A'})
,(b:Page{name:'Site B'})
,(c:Page{name:'Site C'})
,(d:Page{name:'Site D'})
CREATE (home)-[:LINKS]->(about)
,(about)-[:LINKS]->(home)
,(product)-[:LINKS]->(home)
,(home)-[:LINKS]->(product)
,(links)-[:LINKS]->(home)
,(home)-[:LINKS]->(links)
,(links)-[:LINKS]->(a)-[:LINKS]->(home)
,(links)-[:LINKS]->(b)-[:LINKS]->(home)
,(links)-[:LINKS]->(c)-[:LINKS]->(home)
,(links)-[:LINKS]->(d)-[:LINKS]->(home);

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.pageRank.stream('Page', 'LINKS', {iterations:20, dampingFactor:0.85}) 
YIELD node, score 
RETURN node,score order by score desc limit 20;

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.pageRank('Page', 'LINKS', {iterations:20, dampingFactor:0.85, 
write: true,writeProperty:"pagerank"}) 
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty;

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.pageRank(
'MATCH (p:Page) RETURN id(p) as id',
'MATCH (p1:Page)-[:Link]->(p2:Page) RETURN id(p1) as source, id(p2) as target',
{graph:'cypher', iterations:5, write: true});

// end::cypher-loading[]