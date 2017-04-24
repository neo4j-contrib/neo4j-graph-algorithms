package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.impl.PageRankAlgo;
import org.neo4j.graphalgo.serialize.FileSerialization;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class LargeishPageRanker {

    public static void main(final String... args)
    throws IOException, InterruptedException {
        int iterations = 100;
        Path graphDb = null;
        Path prepare = null;
        boolean gcLogs = true;
        boolean useMultiImpl = false;
        boolean multiParallel = false;

        boolean hasErrors = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            try {
                switch (arg) {
                    case "-graphdb":
                        graphDb = Paths.get(args[++i]);
                        break;
                    case "-iterations":
                        iterations = Integer.parseInt(args[++i]);
                        break;
                    case "-prepare":
                        prepare = Paths.get(args[++i]);
                        break;
                    case "-nogclogs":
                        gcLogs = false;
                        break;
                    case "-multi":
                        useMultiImpl = true;
                        break;
                    case "-par":
                        useMultiImpl = true;
                        multiParallel = true;
                        break;
                    case "-help":
                        System.exit(helpAndExit());
                    default:
                        throw new IllegalArgumentException("Unknown parameter " + arg);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                hasErrors = true;
                System.err.println("Missing parameter for argument " + arg);
            } catch (NumberFormatException e) {
                hasErrors = true;
                System.err.println("Invalid parameter for argument " + arg);
            } catch (IllegalArgumentException e) {
                hasErrors = true;
                System.err.println(e.getMessage());
            }
        }

        if (hasErrors) {
            System.exit(helpAndExit(1));
        }
        if (graphDb == null) {
            System.err.println("-graphdb missing");
            System.exit(helpAndExit(2));
        }

        final GcRunner gcRunner = gcLogs ? GcLogger.install() : (ignore) -> System.gc();

        final Graph graph;
        if (prepare != null) {

            final AtomicLong size = new AtomicLong();
            Files.walkFileTree(graphDb, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(
                        final Path file,
                        final BasicFileAttributes attrs)
                throws IOException {
                    size.addAndGet(attrs.size());
                    return super.visitFile(file, attrs);
                }
            });

            runGc("before building graph", gcRunner);

            ExecutorService pool = multiParallel
                    ? Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
//                    ? Executors.newFixedThreadPool(1)
                    : null;

            GraphDatabaseService db = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder(graphDb.toFile())
                    .setConfig(GraphDatabaseSettings.read_only, "true")
                    .setConfig(
                            GraphDatabaseSettings.pagecache_memory,
                            String.valueOf(size.get()))
                    .newGraphDatabase();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (pool != null) {
                    pool.shutdownNow();
                }
                db.shutdown();
            }));

            final long nodeCount;
            final long edgeCount;
            try {
                final GraphDatabaseAPI api = (GraphDatabaseAPI) db;
                try (Transaction tx = db.beginTx();
                     Statement st = api
                             .getDependencyResolver()
                             .resolveDependency(ThreadToStatementContextBridge.class)
                             .get()) {
                    ReadOperations readOperations = st.readOperations();
                    nodeCount = readOperations.nodesGetCount();
                    edgeCount = readOperations.relationshipsGetCount();

                    System.out.printf(
                            "Building Graph with %d nodes and %d relationships...",
                            nodeCount,
                            edgeCount);
                    tx.success();
                }
                long t0 = System.nanoTime();

                final GraphSetup singleThreadSetup = new GraphSetup();
                final GraphSetup multiThreadSetup = new GraphSetup(pool);

                graph = useMultiImpl ?
                        new HeavyGraphFactory(api, multiThreadSetup).build() :
                        new LightGraphFactory(api, singleThreadSetup).build();
                long t1 = System.nanoTime();
                System.out.printf(
                        " done in %.2f seconds%n",
                        TimeUnit.NANOSECONDS.toMillis(t1 - t0) / 1000.0);
            } finally {
                try {
                    if (pool != null) {
                        pool.shutdownNow();
                    }
                    db.shutdown();
                } catch (LifecycleException readOperationsOnReadonlyIndexDuringShutdown) {
                    // ignore
                }
            }

            runGc("after building graph", gcRunner);
            System.out.printf("serializing Graph...");
            long t0 = System.nanoTime();
            FileSerialization.writeGraph(graph, prepare);
            long t1 = System.nanoTime();
            System.out.printf(
                    " done in %.2f seconds%n",
                    TimeUnit.NANOSECONDS.toMillis(t1 - t0) / 1000.0);
            return;
        } else {
            runGc("before loading graph", gcRunner);
            System.out.printf("Loading serialized Graph...");
            long t0 = System.nanoTime();
            graph = useMultiImpl ? FileSerialization.loadHeavyGraph(graphDb) : FileSerialization
                    .loadLightGraph(graphDb);
            long t1 = System.nanoTime();
            System.out.printf(
                    " done in %.2f seconds%n",
                    TimeUnit.NANOSECONDS.toMillis(t1 - t0) / 1000.0);
            runGc("after loading graph", gcRunner);
        }

        System.out.printf(
                "Running PageRank on %d nodes with %d iterations...",
                graph.nodeCount(),
                iterations);
        PageRankAlgo pageRankAlgo = new PageRankAlgo(graph, graph, graph, graph, 0.85);
        long t2 = System.nanoTime();
        double[] ranks = pageRankAlgo.compute(iterations);
        long t3 = System.nanoTime();
        System.out.printf(
                " done in %.2f seconds (that's %.2f seconds per iteration)%n",
                TimeUnit.NANOSECONDS.toMillis(t3 - t2) / 1000.0,
                TimeUnit.NANOSECONDS.toMillis((t3 - t2) / iterations) / 1000.0);

        System.out.println("PageRank for " + ranks.length + " nodes done!");

        runGc("after running PageRank", gcRunner);
    }

    private static void runGc(String reason, GcRunner gcRunner)
    throws InterruptedException {
        gcRunner.runGc("Self initiated GC " + reason);
    }

    private static int helpAndExit() {
        return helpAndExit(0);
    }

    private static int helpAndExit(int code) {
        final StringWriter sw = new StringWriter();
        try (PrintWriter out = new PrintWriter(sw)) {

            //@formatter:off
            out.println( "program -graphdb PATH_TO_GRAPH [options]"                                        );
            out.println(                                                                                   );
            out.println( "  To run PageRank, point -graphdb to the file that contains a serialized graph." );
            out.println( "    e.g. `program -graphdb my.serialized.graph`"                                 );
            out.println(                                                                                   );
            out.println( "  To build a serialized graph, point -graphdb to a Neo4j graph.db file and"      );
            out.println( "    define -prepare to point to the target file for the serialized graph."       );
            out.println( "    e.g. `program -graphdb graph.db -prepare my.serialized.graph`"               );
            out.println(                                                                                   );
            out.println( "  Other options:"                                                                );
            out.println( "       -iterations NUM    | run that many Page Rank iterations"                  );
            out.println( "       -nogclogs          | disable verbose GC logging during execution"         );
            out.println( "       -multi             | use the multi array implementation"                  );
            out.println( "       -par               | use the multi array impl with parallel loading "     );
            out.println( "                          |     uses one thread per cpu; implies -multi"         );
            out.println( "       -help              | show this help screen"                               );
            //@formatter:on
        }

        System.out.println(sw.toString());
        return 64 + code;
    }
}
