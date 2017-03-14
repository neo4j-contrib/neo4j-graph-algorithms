package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.SimpleTimer;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.FileLoaders;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphLoader;
import org.neo4j.graphalgo.core.leightweight.LightGraphLoader;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.logging.Log;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.slf4j.Logger;

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

    private static final Logger LOGGER = getLogger(TestProcedure.class);

    @Procedure("graphalgo.helloworld")
    public void test() {
        log.info("hello world");
        LOGGER.info("test");
    }

    @Procedure("graphalgo.pagerank.heavy")
    public Stream<Ranking> pageRankKernel(@Name("config") Map<String, Object> config) {

        final String label = (String) config.get("label");
        final String relationType = (String) config.get("relation");
        final long iterations = (long) config.get("iterations");
        final double dampingFactor = (double) config.get("dampingFactor");

        final SimpleTimer loading = SimpleTimer.start("loading heavy graph");

        final Graph graph = new HeavyGraphLoader()
                .setLabel(label)
                .setRelation(relationType)
                .load(graphDatabaseAPI);

        loading.stop().dump();

        final SimpleTimer evaluation = SimpleTimer.start("evaluation on heavy graph");

        final double[] ranking = new PageRankKernel(graph, dampingFactor).compute((int) iterations);

        evaluation.stop().dump();

        return IntStream.range(0, ranking.length)
                .mapToObj(index -> new Ranking(graph.toNodeId(index), ranking[index]));
    }

    @Procedure("graphalgo.pagerank.light")
    public Stream<Ranking> pageRankLight(@Name("config") Map<String, Object> config) {

        final String label = (String) config.get("label");
        final String relationType = (String) config.get("relation");
        final long iterations = (long) config.get("iterations");
        final double dampingFactor = (double) config.get("dampingFactor");

        final SimpleTimer loading = SimpleTimer.start("loading light graph");

        final Graph graph = new LightGraphLoader()
                .setLabel(label)
                .setRelation(relationType)
                .load(graphDatabaseAPI);

        loading.stop().dump();

        final SimpleTimer evaluation = SimpleTimer.start("evaluating light graph");

        final double[] ranking = new PageRankKernel(graph, dampingFactor).compute((int) iterations);

        evaluation.stop().dump();

        return IntStream.range(0, ranking.length)
                .mapToObj(index -> new Ranking(graph.toNodeId(index), ranking[index]));
    }

    @Procedure("graphalgo.pagerank.view")
    public Stream<Ranking> pageRankView(@Name("config") Map<String, Object> config) {

        final String label = (String) config.get("label");
        final String relationType = (String) config.get("relation");
        final long iterations = (long) config.get("iterations");
        final double dampingFactor = (double) config.get("dampingFactor");

        final SimpleTimer evaluation = SimpleTimer.start("view evaluation");

        try(GraphView graph = new GraphView(graphDatabaseAPI, label, relationType)) {
            final double[] ranking = new PageRankKernel(graph, dampingFactor).compute((int) iterations);
            evaluation.stop().dump();
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
