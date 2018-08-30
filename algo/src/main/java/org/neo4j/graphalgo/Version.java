package org.neo4j.graphalgo;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

public class Version {
    @UserFunction("algo.version")
    @Description("RETURN algo.version() | return the current graph algorithms installed version")
    public String version() {
        return ""+Version.class.getPackage().getImplementationVersion();
    }
}
