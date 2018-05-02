package org.neo4j.graphalgo.impl.yens;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
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
        applyInTransaction(statement -> {
            final int relId = statement.tokenWrite().relationshipTypeGetOrCreateForName(property);
            if (relId == -1) {
                throw new IllegalStateException("no write property id is set");
            }
            path.forEachEdge((s, t) -> {
                try {
                    statement.dataWrite().relationshipCreate(
                            idMapping.toOriginalNodeId(s),
                            relId,
                            idMapping.toOriginalNodeId(t)
                    );
                } catch (KernelException e) {
                    ExceptionUtil.throwKernelException(e);
                }
            });
            return null;
        });

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
