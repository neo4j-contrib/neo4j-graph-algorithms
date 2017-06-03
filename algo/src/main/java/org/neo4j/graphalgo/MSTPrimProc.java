package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.sources.BothRelationshipAdapter;
import org.neo4j.graphalgo.core.sources.BufferedWeightMap;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;
import org.neo4j.graphalgo.impl.MSTPrim;
import org.neo4j.graphalgo.impl.MSTPrimExporter;
import org.neo4j.graphalgo.results.MSTPrimResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSTPrimProc {

    public static final String CONFIG_WRITE_RELATIONSHIP = "writeProperty";
    public static final String CONFIG_WRITE_RELATIONSHIP_DEFAULT = "mst";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.mst", mode = Mode.WRITE)
    @Description("CALL algo.mst(node:Node, weightProperty:String, {nodeLabelOrQuery:String, relationshipTypeOrQuery:String, " +
            "write:boolean, writeProperty:String stats:boolean}) " +
            "YIELD loadMillis, computeMillis, writeMillis, weightSum, weightMin, weightMax, relationshipCount")
    public Stream<MSTPrimResult> mst(
            @Name("startNode") Node startNode,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        LazyIdMapper idMapper = LazyIdMapper.importer(api)
                .withWeightsFromProperty(weightProperty, 1.0)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .build();

        MSTPrimResult.Builder builder = MSTPrimResult.builder();

        final BufferedWeightMap weightMap;
        final RelationshipContainer relationshipContainer;

        int startNodeId = idMapper.toMappedNodeId(startNode.getId());

        try(ProgressTimer timer = builder.timeLoad()) {
            weightMap = BufferedWeightMap.importer(api)
                    .withIdMapping(idMapper)
                    .withAnyDirection(true)
                    .withWeightsFromProperty(weightProperty, 1.0)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .build();
            relationshipContainer = RelationshipContainer.importer(api)
                    .withIdMapping(idMapper)
                    .withDirection(Direction.BOTH)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .build();
        }

        final MSTPrim mstPrim = new MSTPrim(
                idMapper,
                new BothRelationshipAdapter(relationshipContainer),
                weightMap);

        builder.timeEval(() -> {
            mstPrim.compute(startNodeId);
            if (configuration.isStatsFlag()) {
                MSTPrim.MinimumSpanningTree.Aggregator aggregator =
                        mstPrim.getMinimumSpanningTree().aggregate();
                builder.withWeightMax(aggregator.getMax())
                        .withWeightMin(aggregator.getMin())
                        .withWeightSum(aggregator.getSum())
                        .withRelationshipCount(aggregator.getCount());
            }
        });

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new MSTPrimExporter(api)
                        .withIdMapping(idMapper)
                        .withWriteRelationship(configuration.get(CONFIG_WRITE_RELATIONSHIP, CONFIG_WRITE_RELATIONSHIP_DEFAULT))
                        .write(mstPrim.getMinimumSpanningTree());
            });
        }

        return Stream.of(builder.build());
    }
}
