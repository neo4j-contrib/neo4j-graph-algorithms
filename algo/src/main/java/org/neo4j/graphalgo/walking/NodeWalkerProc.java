package org.neo4j.graphalgo.walking;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.NodeWalker;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphalgo.results.VirtualRelationship;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class NodeWalkerProc  {

    private static final RelationshipType NEXT = RelationshipType.withName("NEXT");
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;



    @Procedure(name = "algo.randomWalk.stream", mode = Mode.READ)
    @Description("CALL algo.randomWalk.stream(label, relationship, start:null=all/[ids]/label, steps, walks, {mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH'}) " +
            "YIELD nodes, path - computes random walks from given starting points")
    public Stream<WalkResult> randomWalk(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "start", defaultValue = "null") Object start,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }


        Number returnParam = configuration.get("return", 1d);
        Number inOut = configuration.get("inOut", 1d);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        Boolean returnPath = configuration.get("path", false);

        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);

        PrimitiveIterator.OfInt idStream = IntStream.range(0, limit).unordered().parallel().flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator();

        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
        return randomWalks
                .map( nodes -> new WalkResult(nodes, returnPath ? toPath(api, nodes,direction) : null));
    }



    private Path toPath(GraphDatabaseAPI api, long[] nodes, Direction direction) {
        if (nodes.length == 0) return WalkPath.EMPTY;
        WalkPath result = new WalkPath(nodes.length);
        Node node = api.getNodeById(nodes[0]);
        result.addNode(node);
        for (int i = 1; i < nodes.length; i++) {
            Node nextNode = api.getNodeById(nodes[i]);
            result.addRelationship(new VirtualRelationship(node, nextNode, NEXT));
            // result.addRelationship(findRelationship(direction, result.endNode(), nextNode)); // todo use virtual relationship?
            result.addNode(nextNode);
            node = nextNode;
        }
        return result;
    }

    private Relationship findRelationship(Direction direction, Node start, Node end) {
        for (Relationship rel : start.getRelationships(direction)) {
            if (rel.getOtherNode(start).equals(end)) return rel;
        }
        return null;
    }

    private IntStream idStream(@Name(value = "start", defaultValue = "null") Object start, Graph graph, int limit) {
        int nodeCount = Math.toIntExact(graph.nodeCount());
        if (start instanceof String) {
            String label = start.toString();
            int labelId = transaction.tokenRead().nodeLabel(label);
            int countWithLabel = Math.toIntExact(transaction.dataRead().countsForNodeWithoutTxState(labelId));
            NodeLabelIndexCursor cursor = transaction.cursors().allocateNodeLabelIndexCursor();
            transaction.dataRead().nodeLabelScan(labelId, cursor);
            cursor.next();
            LongStream ids;
            if (limit == -1) {
                ids = LongStream.range(0, countWithLabel).map( i -> cursor.next() ? cursor.nodeReference() : -1L );
            } else {
                int[] indexes = ThreadLocalRandom.current().ints(limit + 1, 0, countWithLabel).sorted().toArray();
                IntStream deltas = IntStream.range(0, limit).map(i -> indexes[i + 1] - indexes[i]);
                ids = deltas.mapToLong(delta -> { while (delta > 0 && cursor.next()) delta--;return cursor.nodeReference(); });
            }
            return ids.mapToInt(graph::toMappedNodeId).onClose(cursor::close);
        } else if (start instanceof Collection) {
            return ((Collection)start).stream().mapToLong(e -> ((Number)e).longValue()).mapToInt(graph::toMappedNodeId);
        } else if (start instanceof Number) {
            return LongStream.of(((Number)start).longValue()).mapToInt(graph::toMappedNodeId);
        } else {
            if (nodeCount < limit) {
                return IntStream.range(0,nodeCount).limit(limit);
            } else {
                return IntStream.generate(() -> ThreadLocalRandom.current().nextInt(nodeCount)).limit(limit);
            }
        }
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder, ProcedureConfiguration configuration) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withDirection(configuration.getDirection(Direction.BOTH))
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutRelationshipWeights();


        try (ProgressTimer timer = ProgressTimer.start()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

}
