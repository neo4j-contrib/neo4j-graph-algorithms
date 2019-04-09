package org.neo4j.graphalgo.core.utils;

final class ConcurrencyConfig {

    private static final String PROCESSORS_OVERRIDE_PROPERTY = "neo4j.graphalgo.processors";
    private static final int MAX_CE_CONCURRENCY = 4;

    final int maxConcurrency;
    final int defaultConcurrency;

    static ConcurrencyConfig of() {
        Integer definedProcessors = null;
        try {
            definedProcessors = Integer.getInteger(PROCESSORS_OVERRIDE_PROPERTY);
        } catch (SecurityException ignored) {
        }
        if (definedProcessors == null) {
            definedProcessors = Runtime.getRuntime().availableProcessors();
        }
        boolean isOnEnterprise = Package.getPackage("org.neo4j.kernel.impl.enterprise") != null;
        return new ConcurrencyConfig(definedProcessors, isOnEnterprise);
    }

    /* test-private */ ConcurrencyConfig(int availableProcessors, boolean isOnEnterprise) {
        if (isOnEnterprise) {
            maxConcurrency = Integer.MAX_VALUE;
            defaultConcurrency = availableProcessors;
        } else {
            maxConcurrency = MAX_CE_CONCURRENCY;
            defaultConcurrency = Math.min(availableProcessors, MAX_CE_CONCURRENCY);
        }
    }
}
