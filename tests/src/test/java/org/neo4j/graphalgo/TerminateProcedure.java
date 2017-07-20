package org.neo4j.graphalgo;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
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
    public KernelTransaction ktx;

    @Procedure("test.testProc")
    public void allShortestPathsStream(
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) throws TransactionFailureException {

        // simulate long running algorithm
        while (true) {

            // check if termination reason is set
            Status reason = ktx.getReasonIfTerminated();
            if (reason == null) {

                // check if ktx is still open
                if (!ktx.isOpen()) {
                    System.out.println("KernelTransaction is closed");
                    ktx.failure();
                    break;
                }

            } else {
                System.out.println("reason.code().description() = " + reason.code().description());
                ktx.failure();
                break;
            }
        }

        System.out.println("transaction termination successful");

    }

    public static class Result {
        public final long id;
        public Result(long id) {
            this.id = id;
        }
    }
}
