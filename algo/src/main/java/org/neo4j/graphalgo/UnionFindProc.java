package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.sources.SingleRunAllRelationIterator;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.UnionFind;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class UnionFindProc {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Procedure(value = "org.neo4j.graphalgo.unionFind")
    @Description("CALL org.neo4j.graphalgo.unionFind() YIELD nodeId, setId - calculates page rank based on cypher input")

    public Stream<DisjointSetStruct.Result> unionFind() {

        final LazyIdMapper idMapping = LazyIdMapper.importer(db)
                .withAnyLabel()
                .build();

        final SingleRunAllRelationIterator relationIterator = new SingleRunAllRelationIterator(
                db,
                idMapping);

        return new UnionFind(idMapping, relationIterator)
                .compute()
                .mapAndStream(idMapping);
    }
}
