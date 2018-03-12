
// tag::hotel-reviewers-pagerank[]
CALL algo.pageRank(
    "MATCH (u:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
           (:Category {name: 'Hotels'})
     WITH u, count(*) AS reviews
     WHERE reviews > 5
     RETURN id(u) AS id",
    "MATCH (u1:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
           (:Category {name: 'Hotels'})
     MATCH (u1)-[:FRIENDS]->(u2)
     WHERE id(u1) < id(u2)
     RETURN id(u1) AS source, id(u2) AS target",
    {graph: "cypher", write: true, direction: "both", writeProperty: "hotelPageRank"}
)
// end::hotel-reviewers-pagerank[]

// tag::top-reviewers[]

MATCH (u:User)
WHERE u.hotelPageRank > 0
WITH u
ORDER BY u.hotelPageRank DESC
LIMIT 5
RETURN u.name AS name,
       apoc.math.round(u.hotelPageRank,2) AS pageRank,
       size((u)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
            (:Category {name: "Hotels"})) AS hotelReviews,
       size((u)-[:WROTE]->()) AS totalReviews,
       size((u)-[:FRIENDS]-()) AS friends


// end::top-reviewers[]


// tag::caesars[]

MATCH (b:Business {name: "Caesars Palace Las Vegas Hotel & Casino"})
      <-[:REVIEWS]-(review)-[:WROTE]-(user)
RETURN user.name AS name,
       apoc.math.round(user.hotelPageRank,2) AS pageRank,
       review.stars AS stars
ORDER BY user.hotelPageRank DESC
LIMIT 5


// end::caesars[]

// tag::eda[]

CALL db.labels()
YIELD label
CALL apoc.cypher.run("MATCH (:`"+label+"`) RETURN count(*) as count", null)
YIELD value
RETURN label, value.count as count

// end::eda[]

// tag::eda-rels[]

CALL db.relationshipTypes()
YIELD relationshipType
CALL apoc.cypher.run("MATCH ()-[:" + `relationshipType` + "]->() RETURN count(*) as count", null)
YIELD value
RETURN relationshipType, value.count AS count

// end::eda-rels[]
