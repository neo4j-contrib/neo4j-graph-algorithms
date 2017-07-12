package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Name;

import java.util.Map;

abstract class AbstractBaseProc {

    ProcedureConfiguration readConfig(
            String label,
            String relationship,
            Map<String, Object> config) {
        return ProcedureConfiguration
                .create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);
    }

    GraphLoader graphLoader(
            GraphDatabaseAPI api,
            ProcedureConfiguration config) {
        return new GraphLoader(api)
                .withOptionalLabel(config.getNodeLabelOrQuery())
                .withOptionalRelationshipType(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getProperty(),
                        config.getPropertyDefaultValue(1.0))
                .withExecutorService(Pools.DEFAULT);
    }

    final Graph loadGraph(
            GraphDatabaseAPI api,
            ProcedureConfiguration config,
            AbstractResultBuilder<?> resultBuilder) {
        return resultBuilder.timeLoad(() -> loadGraph(api, config));
    }

    final Graph loadGraph(
            GraphDatabaseAPI api,
            ProcedureConfiguration config) {
        Object existingGraph = config.get(ProcedureConstants.GRAPH_IMPL_PARAM);
        if (existingGraph instanceof Graph) {
            return (Graph) existingGraph;
        }
        GraphLoader loader = graphLoader(api, config);
        Class<? extends GraphFactory> graphImpl = config.getGraphImpl();
        return loader.load(graphImpl);
    }
}
