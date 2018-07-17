package org.neo4j.graphalgo.impl.yens;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Specialized exporter for {@link WeightedPath}
 *
 * @author mknblch
 */
public class WeightedPathExporter extends StatementApi {

    private final IdMapping idMapping;
    private final RelationshipWeights relationshipWeights;
    private final String relPrefix;
    private final ExecutorService executorService;
    private final String propertyName;

    public WeightedPathExporter(GraphDatabaseAPI api,
                                ExecutorService executorService,
                                IdMapping idMapping,
                                RelationshipWeights relationshipWeights,
                                String relPrefix,
                                String propertyName) {
        super(api);
        this.executorService = executorService;
        this.idMapping = idMapping;
        this.relationshipWeights = relationshipWeights;
        this.relPrefix = relPrefix;
        this.propertyName = propertyName;
    }

    /**
     * export a list of weighted paths
     * @param paths
     */
    public void export(List<WeightedPath> paths) {
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(paths);
        } else {
            writeSequential(paths);
        }
    }

    private void export(String relationshipType, String propertyName, WeightedPath path) {
        applyInTransaction(statement -> {
            final int relId = statement.tokenWrite().relationshipTypeGetOrCreateForName(relationshipType);
            if (relId == -1) {
                throw new IllegalStateException("no write property id is set");
            }
            path.forEachEdge((s, t) -> {
                try {
                    long relationshipId = statement.dataWrite().relationshipCreate(
                            idMapping.toOriginalNodeId(s),
                            relId,
                            idMapping.toOriginalNodeId(t)
                    );

                    statement.dataWrite().relationshipSetProperty(
                            relationshipId,
                            getOrCreatePropertyId(propertyName),
                            Values.doubleValue(relationshipWeights.weightOf(s, t)));
                } catch (KernelException e) {
                    ExceptionUtil.throwKernelException(e);
                }
            });
            return null;
        });

    }

    private int getOrCreatePropertyId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .propertyKeyGetOrCreateForName(propertyName));
    }

    private void writeSequential(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);
        paths.stream()
                .sorted(WeightedPath.comparator())
                .forEach(path ->
                        export(String.format("%s%d", relPrefix, counter.v++), propertyName, path));
    }

    private void writeParallel(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);
        final List<Runnable> tasks = paths.stream()
                .sorted(WeightedPath.comparator())
                .map(path -> (Runnable) () ->
                        export(String.format("%s%d", relPrefix, counter.v++), propertyName, path))
                .collect(Collectors.toList());
        ParallelUtil.run(tasks, executorService);
    }

}
