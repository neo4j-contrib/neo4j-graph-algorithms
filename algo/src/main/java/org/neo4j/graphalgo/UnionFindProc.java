package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class UnionFindProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "org.neo4j.graphalgo.unionFind")
    @Description("CALL org.neo4j.graphalgo.unionFind(label:String, relationship:String) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFind(
            @Name("label") String label,
            @Name("relationship") String relationship) {

        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withRelationshipType(relationship)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        return new GraphUnionFind(graph)
                .compute()
                .resultStream(graph);
    }

    @Procedure(value = "org.neo4j.graphalgo.unionFindThreshold")
    @Description("CALL org.neo4j.graphalgo.unionFindThreshold(label:String, relationship:String, property:String, threshold:double) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindThreshold(
            @Name("label") String label,
            @Name("relationship") String relationship,
            @Name("property") String property,
            @Name("threshold") double threshold) {

        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withRelationshipType(relationship)
                .withWeightsFromProperty(property, 0.0)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        return new GraphUnionFind(graph)
                .compute(threshold)
                .resultStream(graph);
    }
}
