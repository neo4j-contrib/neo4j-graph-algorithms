package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.results.AbstractWriteBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CentralityUtils {
    public static void write(GraphDatabaseAPI api, Log log, Graph graph, TerminationFlag terminationFlag,
                             CentralityResult result, ProcedureConfiguration configuration,
                             AbstractWriteBuilder statsBuilder,
                             String defaultScoreProperty) {
        if (configuration.isWriteFlag(true)) {
            log.debug("Writing results");
            String propertyName = configuration.getWriteProperty(defaultScoreProperty);
            try (ProgressTimer timer = statsBuilder.timeWrite()) {
                Exporter exporter = Exporter
                        .of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build();
                result.export(propertyName, exporter);
            }
            statsBuilder.withWrite(true).withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }

    static Stream<CentralityScore> streamResults(Graph graph, CentralityResult scores) {
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return LongStream.range(0, hugeGraph.nodeCount())
                    .mapToObj(i -> {
                        final long nodeId = hugeGraph.toOriginalNodeId(i);
                        return new CentralityScore(
                                nodeId,
                                scores.score(i)
                        );
                    });
        }

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
                .mapToObj(i -> {
                    final long nodeId = graph.toOriginalNodeId(i);
                    return new CentralityScore(
                            nodeId,
                            scores.score(i)
                    );
                });
    }
}
