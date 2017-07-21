package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;

/**
 * @author mknblch
 */
public class TerminateProcedure {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("test.testProc")
    public void allShortestPathsStream(
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) throws TransactionFailureException {

        final TerminationFlag flag = TerminationFlag.wrap(transaction);
        while (flag.running()) {
            // simulate long running algorithm
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {}
        }
        log.info("algorithm termination successful");
    }

    public static class Result {
        public final long id;
        public Result(long id) {
            this.id = id;
        }
    }
}
