package org.neo4j.graphalgo.impl.yens;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

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
    private final String relPrefix;
    private final ExecutorService executorService;

    public WeightedPathExporter(GraphDatabaseAPI api,
                                ExecutorService executorService,
                                IdMapping idMapping,
                                String relPrefix) {
        super(api);
        this.executorService = executorService;
        this.idMapping = idMapping;
        this.relPrefix = relPrefix;
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

    private void export(String property, WeightedPath path) {
        try {
            applyInTransaction(statement -> {
                final DataWriteOperations op = statement.dataWriteOperations();
                final int relId = statement.tokenWriteOperations()
                        .relationshipTypeGetOrCreateForName(property);
                path.forEachEdge((s, t) -> {
                    try {
                        op.relationshipCreate(relId,
                        idMapping.toOriginalNodeId(s),
                        idMapping.toOriginalNodeId(t));
                    } catch (RelationshipTypeIdNotFoundKernelException | EntityNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                });
                return null;
            });
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeSequential(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);
        paths.stream()
                .sorted(WeightedPath.comparator())
                .forEach(path ->
                        export(String.format("%s%d", relPrefix, counter.v++), path));
    }

    private void writeParallel(List<WeightedPath> paths) {
        final Pointer.IntPointer counter = Pointer.wrap(0);
        final List<Runnable> tasks = paths.stream()
                .sorted(WeightedPath.comparator())
                .map(path -> (Runnable) () ->
                        export(String.format("%s%d", relPrefix, counter.v++), path))
                .collect(Collectors.toList());
        ParallelUtil.run(tasks, executorService);
    }

}
