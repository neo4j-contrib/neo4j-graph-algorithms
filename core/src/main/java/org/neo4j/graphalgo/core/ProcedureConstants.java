package org.neo4j.graphalgo.core;

import java.util.Locale;

/**
 * @author mknblch
 */
public class ProcedureConstants {

    public static final String CYPHER_QUERY = "cypher";

    public static final String NODE_LABEL_QUERY_PARAM = "nodeQuery";

    public static final String RELATIONSHIP_QUERY_PARAM = "relationshipQuery";

    public static final String PROPERTY_PARAM = "weightProperty";

    public static final String PROPERTY_PARAM_DEFAULT = "weight";

    public static final String DEFAULT_PROPERTY_VALUE_PARAM = "defaultValue";

    public static final String WRITE_FLAG = "write";

    public static final String WRITE_PROPERTY = "writeProperty";

    public static final String WRITE_PROPERTY_DEFAULT = "value";

    public static final String STATS_FLAG = "stats";

    public static final double DEFAULT_PROPERTY_VALUE_DEFAULT = 1.0;

    public static final String ITERATIONS_PARAM = "iterations";

    public static final int ITERATIONS_DEFAULT = 1;

    public static final String BATCH_SIZE_PARAM = "batchSize";

    public static final String DIRECTION = "direction";

    public static final String DIRECTION_DEFAULT = "BOTH";

    public static final String GRAPH_IMPL_PARAM = "graph";

    public static final String DEFAULT_GRAPH_IMPL = "heavy";

    public static final String CONCURRENCY = "concurrency";

    public static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors() / 2;
}
