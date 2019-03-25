package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@State(Scope.Benchmark)
public class InfoMapGraph {

    private GraphDatabaseAPI db;
    Graph graph;
    ProgressLogger progressLogger;
    NodeWeights pageRanks;

    final int concurrency = 2;
    final double tau = 0.3;
    final int iterations = 15;
    final double threshold = 0.01;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb("Yelp");

        Log log = FormattedLog.withLogLevel(Level.DEBUG).toOutputStream(System.out);
        AllocationTracker tracker = AllocationTracker.create();

        graph = new GraphLoader(db, Pools.DEFAULT)
                .withConcurrency(concurrency)
                .withAllocationTracker(tracker)
                .withLog(log)
                .withLogInterval(1L, TimeUnit.SECONDS)
                .asUndirected(true)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);


        CentralityResult pr = PageRankAlgorithm.of(
                tracker,
                graph,
                1.0 - tau,
                LongStream.empty(),
                Pools.DEFAULT,
                concurrency,
                ParallelUtil.DEFAULT_BATCH_SIZE
        ).compute(iterations).result();

        long[] allRels = new long[1];
        graph.forEachNode(node -> {
            allRels[0] += (long) (graph.degree(node, Direction.OUTGOING));
            return true;
        });

        log.info("Graph with %,d nodes and %,d relationships", graph.nodeCount(), allRels[0]);

        progressLogger = ProgressLogger.wrap(log, "InfoMap", 1L, TimeUnit.SECONDS);
        pageRanks = pr::score;
    }

    @TearDown
    public void shutdown() {
        graph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }
}
