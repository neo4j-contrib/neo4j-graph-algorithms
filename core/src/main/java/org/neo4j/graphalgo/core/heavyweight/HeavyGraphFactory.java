package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class HeavyGraphFactory extends GraphFactory {

    public HeavyGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        return build(setup.batchSize);
    }

    /* test-private */ Graph build(int batchSize) {
        try {
            return importGraph(batchSize);
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }

    private Graph importGraph(final int batchSize) throws
            EntityNotFoundException {
        final IdMap idMap = loadIdMap();

        final Supplier<WeightMapping> relWeights = () -> newWeightMap(
                dimensions.relWeightId(),
                setup.relationDefaultWeight);
        final Supplier<WeightMapping> nodeWeights = () -> newWeightMap(
                dimensions.nodeWeightId(),
                setup.nodeDefaultWeight);
        final Supplier<WeightMapping> nodeProps = () -> newWeightMap(
                dimensions.nodePropId(),
                setup.nodeDefaultPropertyValue);

        int concurrency = setup.concurrency();
        final int nodeCount = dimensions.nodeCount();
        int actualBatchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                batchSize);
        Collection<RelationshipImporter> tasks = ParallelUtil.readParallel(
                concurrency,
                actualBatchSize,
                idMap,
                (offset, nodeIds) -> new RelationshipImporter(
                        api,
                        setup,
                        dimensions,
                        progress,
                        actualBatchSize,
                        offset,
                        idMap,
                        nodeIds,
                        relWeights,
                        nodeWeights,
                        nodeProps
                ),
                threadPool);

        final Graph graph = buildCompleteGraph(
                nodeCount,
                idMap,
                relWeights,
                nodeWeights,
                nodeProps,
                tasks);

        progressLogger.logDone();
        return graph;
    }

    private Graph buildCompleteGraph(
            int nodeCount,
            final IdMap idMap,
            final Supplier<WeightMapping> relWeightsSupplier,
            final Supplier<WeightMapping> nodeWeightsSupplier,
            final Supplier<WeightMapping> nodePropsSupplier,
            Collection<RelationshipImporter> tasks) {
        if (tasks.size() == 1) {
            RelationshipImporter importer = tasks.iterator().next();
            return importer.toGraph(idMap);
        }

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);
        final WeightMapping relWeights = relWeightsSupplier.get();
        final WeightMapping nodeWeights = nodeWeightsSupplier.get();
        final WeightMapping nodeProps = nodePropsSupplier.get();
        for (RelationshipImporter task : tasks) {
            task.writeInto(matrix, relWeights, nodeWeights, nodeProps);
            task.release();
        }

        return new HeavyGraph(
                idMap,
                matrix,
                relWeights,
                nodeWeights,
                nodeProps);
    }
}
