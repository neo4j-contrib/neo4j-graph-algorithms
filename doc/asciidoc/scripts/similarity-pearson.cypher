// tag::function[]
RETURN algo.similarity.pearson([5,8,7,5,4,9], [7,8,6,6,4,5]) AS similarity
// end::function[]


// tag::create-sample-graph[]

MERGE (home_alone:Movie {name:'Home Alone'})
MERGE (matrix:Movie {name:'The Matrix'})
MERGE (good_men:Movie {name:'A Few Good Men'})
MERGE (top_gun:Movie {name:'Top Gun'})
MERGE (jerry:Movie {name:'Jerry Maguire'})

MERGE (zhen:Person {name: "Zhen"})
MERGE (praveena:Person {name: "Praveena"})
MERGE (michael:Person {name: "Michael"})
MERGE (arya:Person {name: "Arya"})
MERGE (karin:Person {name: "Karin"})

MERGE (praveena)-[:RATED {score: 9}]->(good_men)
MERGE (praveena)-[:RATED {score: 5}]->(jerry)
MERGE (praveena)-[:RATED {score: 3}]->(home_alone)

MERGE (zhen)-[:RATED {score: 3}]->(home_alone)
MERGE (zhen)-[:RATED {score: 8}]->(good_men)
MERGE (zhen)-[:RATED {score: 9}]->(matrix)

MERGE (michael)-[:RATED {score: 8}]->(home_alone)
MERGE (michael)-[:RATED {score: 3}]->(matrix)
MERGE (michael)-[:RATED {score: 9}]->(good_men)

MERGE (arya)-[:RATED {score: 3}]->(top_gun)
MERGE (arya)-[:RATED {score: 10}]->(matrix)
MERGE (arya)-[:RATED {score: 7}]->(jerry)

MERGE (karin)-[:RATED {score: 9}]->(top_gun)
MERGE (karin)-[:RATED {score: 7}]->(matrix)

MERGE (michael)-[:RATED {score: 7}]->(home_alone)
MERGE (michael)-[:RATED {score: 9}]->(good_men)

// end::create-sample-graph[]

// tag::stream[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, 0))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data)
YIELD item1, item2, count1, count2, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream[]

// tag::stream-similarity-cutoff[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, 0))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, 0))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson.stream(data, {topK:1, similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, similarity
ORDER BY similarity DESC
// end::stream-topk[]

// tag::write-back[]
MATCH (p:Person), (m:Movie)
OPTIONAL MATCH (p)-[rated:RATED]->(m)
WITH {item:id(p), weights: collect(coalesce(rated.score, 0))} as userData
WITH collect(userData) as data
CALL algo.similarity.pearson(data, {topK: 1, similarityCutoff: 0.1, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH (p:Person {name: "Karin"})-[:SIMILAR]->(other),
      (other)-[r:RATED]->(movie)
WHERE not((p)-[:RATED]->(movie)) and r.score >= 8
RETURN movie.name AS movie
// end::query[]