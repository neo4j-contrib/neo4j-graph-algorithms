package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;

public final class LoadGraphProc extends AbstractBaseProc {

    @Context
    public GraphDatabaseAPI api;

    @UserFunction("algo.loadGraph")
    @Description("CALL algo.loadGraph(" +
            "label:String, " +
            "relationship:String, " +
            "{weightProperty:'weight}) " +
            "YIELD graph, nodes, loadMillis - " +
            "loads a graph and returns it")
    public Object pageRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return loadGraph(api, readConfig(label, relationship, config));
    }

    @Override
    GraphLoader graphLoader(
            GraphDatabaseAPI api,
            ProcedureConfiguration config) {
        return super.graphLoader(api, config).withDirection(config.getDirection());
    }
}
