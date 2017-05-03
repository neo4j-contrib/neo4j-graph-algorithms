package org.neo4j.graphalgo;

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
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSTPrimProc {

    public static final String CONFIG_STATS = "stats";
    public static final String CONFIG_WRITE = "write";
    public static final String CONFIG_WRITE_RELATIONSHIP = "writeProperty";
    public static final String CONFIG_WRITE_RELATIONSHIP_DEFAULT = "mst";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.mstprim", mode = Mode.WRITE)
    @Description("CALL algo.mstprim(node:Node, property:String, {label:String, relationship:String, write:boolean, writeProperty:String stats:boolean}) " +
            "YIELD loadDuration, evalDuration, writeDuration, weightSum, weightMin, weightMax, relationshipCount")
    public Stream<MSTPrimResult> mstPrim(
            @Name("startNode") Node startNode,
            @Name(value = "property") String propertyName,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        LazyIdMapper idMapper = new LazyIdMapper();

        MSTPrimResult.Builder builder = MSTPrimResult.builder();

        ProgressTimer timer = ProgressTimer.start(builder::withLoadDuration);
        BufferedWeightMap weightMap = BufferedWeightMap.importer(api)
                .withIdMapping(idMapper)
                .withAnyDirection(true)
                .withWeightsFromProperty(propertyName, 1.0)
                .withOptionalLabel((String) config.get("label"))
                .withOptionalRelationshipType((String) config.get("relationship"))
                .build();

        RelationshipContainer relationshipContainer = RelationshipContainer.importer(api)
                .withIdMapping(idMapper)
                .withDirection(Direction.BOTH)
                .withOptionalLabel((String) config.get("label"))
                .withOptionalRelationshipType((String) config.get("relationship"))
                .build();

        timer.stop();


        int startNodeId = idMapper.toMappedNodeId(startNode.getId());

        timer = ProgressTimer.start(builder::withEvalDuration);
        final MSTPrim mstPrim = new MSTPrim(idMapper,
                new BothRelationshipAdapter(relationshipContainer),
                weightMap)
                .compute(startNodeId);

        if ((Boolean) config.getOrDefault(CONFIG_STATS, Boolean.FALSE)) {
            MSTPrim.MinimumSpanningTree.Aggregator aggregator =
                    mstPrim.getMinimumSpanningTree().aggregate();
            builder.withWeightMax(aggregator.getMax())
                    .withWeightMin(aggregator.getMin())
                    .withWeightSum(aggregator.getSum())
                    .withRelationshipCount(aggregator.getCount());

        }
        timer.stop();

        if ((Boolean) config.getOrDefault(CONFIG_WRITE, Boolean.FALSE)) {
            timer = ProgressTimer.start(builder::withWriteDuration);
            new MSTPrimExporter(api)
                    .withIdMapping(idMapper)
                    .withWriteRelationship((String) config.getOrDefault(CONFIG_WRITE_RELATIONSHIP, CONFIG_WRITE_RELATIONSHIP_DEFAULT))
                    .write(mstPrim.getMinimumSpanningTree());
            timer.stop();
        }

        return Stream.of(builder.build());
    }
}
