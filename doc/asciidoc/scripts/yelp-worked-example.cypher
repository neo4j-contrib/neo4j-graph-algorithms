
// tag::hotel-reviewers-pagerank[]
CALL algo.pageRank(
    "MATCH (u:User)-[:WROTE]->()-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(:Category {name: 'Hotels'})
     WITH u, count(*) AS reviews
     WHERE reviews > 5
     RETURN id(u) AS id",
    "MATCH (u1:User)-[:WROTE]->()-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(:Category {name: 'Hotels'})
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
RETURN u.name AS user,
       u.hotelPageRank AS pageRank,
       size((u)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->
            (:Category {name: "Hotels"})) AS hotelReviews,
       size((u)-[:WROTE]->()) AS totalReviews,
       size((u)-[:FRIENDS]-()) AS friends


// end::top-reviewers[]


// tag::caesars[]

MATCH (b:Business {name: "Caesars Palace Las Vegas Hotel & Casino"})
      <-[:REVIEWS]-(review)-[:WROTE]-(user)
RETURN user.name AS user, user.hotelPageRank AS pageRank, review.stars AS stars
ORDER BY user.hotelPageRank DESC
LIMIT 10


// end::caesars[]
