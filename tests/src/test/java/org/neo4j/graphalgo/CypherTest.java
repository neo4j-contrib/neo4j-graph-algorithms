package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class CypherTest {

    private final String cypher;
    private GraphDatabaseAPI api;
    private final GraphLoader loader;

    public CypherTest(String cypher) {
        this.cypher = cypher;
        api = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        loader = new GraphLoader(api);
    }

    public CypherTest withLoader(Consumer<GraphLoader> loaderFunction) {
        loaderFunction.accept(loader);
        return this;
    }

    public GraphDatabaseAPI getApi() {
        return api;
    }

    public Node findNode(String label, String key, Object value) {
        try (Transaction tx = api.beginTx()) {
            Node node = api.findNode(Label.label(label), key, value);
            tx.success();
            return node;
        }
    }

    public Graph build() {
        return build(HeavyGraphFactory.class);
    }

    public Graph build(Class<? extends GraphFactory> factory) {
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.printf("import took %d ms%n", l))) {
            try (Transaction tx = api.beginTx()) {
                api.execute(cypher).close();
                tx.success();
            }
        }
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.printf("load took %d ms%n", l))) {
            return loader.load(factory);
        }
    }
}
