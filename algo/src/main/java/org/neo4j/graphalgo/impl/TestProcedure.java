package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphLoader;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.logging.Log;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mknobloch
 */
public class TestProcedure {

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public Log log;

    @Procedure("graphalgo.helloworld")
    public void test() {
        log.info("hello world");
    }

    @Procedure("graphalgo.pagerank.kernel")
    public Stream<Ranking> pageRankKernel(@Name("config") Map<String, Object> config) {

        final String label = (String) config.get("label");
        final String relationType = (String) config.get("relationType");
        final long iterations = (long) config.get("iterations");
        final double dampingFactor = (double) config.get("dampingFactor");

        final WeightedGraph graph = new HeavyGraphLoader()
                .setLabel(label)
                .setRelationType(relationType)
                .load(graphDatabaseAPI);

        final double[] ranking = new PageRankKernel(graph, dampingFactor).compute((int) iterations);
        return IntStream.range(0, ranking.length)
                .mapToObj(index -> new Ranking(graph.toNodeId(index), ranking[index]));
    }


    @Procedure("graphalgo.pagerank.view")
    public Stream<Ranking> pageRankView(@Name("config") Map<String, Object> config) {

        final String label = (String) config.get("label");
        final String relationType = (String) config.get("relationType");
        final long iterations = (long) config.get("iterations");
        final double dampingFactor = (double) config.get("dampingFactor");

        try(GraphView graph = new GraphView(graphDatabaseAPI, relationType)) {
            final double[] ranking = new PageRankKernel(graph, dampingFactor).compute((int) iterations);
            return IntStream.range(0, ranking.length)
                    .mapToObj(index -> new Ranking(graph.toNodeId(index), ranking[index]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class Ranking {

        public long nodeId;
        public double pageRank;

        public Ranking(long nodeId, double pageRank) {
            this.nodeId = nodeId;
            this.pageRank = pageRank;
        }
    }
}
