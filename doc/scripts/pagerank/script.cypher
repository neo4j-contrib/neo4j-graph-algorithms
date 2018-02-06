// tag::import-nodes[]
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
MERGE (:Page {name: row.page})
// end::import-nodes[]

// tag::import-relationships[]
LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
MATCH (p1:Page {name: row.source})
MATCH (p2:Page {name: row.target})
MERGE (p1)-[:LINKS]->(p2)
// end::import-relationships[]

// tag::algorithm[]
CALL algo.pageRank.stream('Page', 'LINKS', {iterations:20, dampingFactor:0.85})
YIELD node, score
RETURN node.name,score
ORDER BY score DESC
// end::algorithm[]

// tag::result[]
╒═════════╤═════════╕
│"page"   │"score"  │
╞═════════╪═════════╡
│"Home"   │1.6409935│
├─────────┼─────────┤
│"About"  │0.614712 │
├─────────┼─────────┤
│"Product"│0.614712 │
├─────────┼─────────┤
│"Links"  │0.614712 │
├─────────┼─────────┤
│"Site A" │0.25438  │
├─────────┼─────────┤
│"Site B" │0.25438  │
├─────────┼─────────┤
│"Site C" │0.25438  │
├─────────┼─────────┤
│"Site D" │0.25438  │
└─────────┴─────────┘
// end::result[]
