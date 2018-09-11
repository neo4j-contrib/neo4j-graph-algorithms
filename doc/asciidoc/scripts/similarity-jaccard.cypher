// tag::function[]
RETURN algo.similarity.jaccard([1,2,3], [1,2,4,5]) AS similarity
// end::function[]

// tag::create-sample-graph[]

MERGE (french:Cuisine {name:'French'})
MERGE (italian:Cuisine {name:'Italian'})
MERGE (indian:Cuisine {name:'Indian'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})

MERGE (praveena)-[:LIKES]->(indian)
MERGE (zhen)-[:LIKES]->(french)
MERGE (michael)-[:LIKES]->(french)
MERGE (michael)-[:LIKES]->(italian);

// end::create-sample-graph[]

// tag::stream[]
MATCH (p:Person)-[:LIKED]->(cuisine)
WITH {source:id(p), targets: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data)
YIELD source1, source2, count1, count2, intersection, similarity
WHERE source1 < source2
WITH algo.getNodeById(source1) AS from, algo.getNodeById(source2) AS to
RETURN from.name AS from, to.name AS to, similarity
// end::stream[]

// tag::stream-topk[]
MATCH (p:Person)-[:LIKED]->(cuisine)
WITH {source:id(p), targets: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data, {topK:3, similarityCutoff:0.5, degreeCutoff:10})
YIELD source1, source2, count1, count2, intersection, similarity
WHERE source1 < source2
WITH algo.getNodeById(source1) AS from, algo.getNodeById(source2) AS to
RETURN from.name AS from, to.name AS to, similarity
// end::stream-topk[]

// tag::write-back[]

// end::write-back[]

// tag::query[]

// end::query[]
