package org.neo4j.graphalgo.core.utils;

final class ConcurrencyConfig {

    private static final String PROCESSORS_OVERRIDE_PROPERTY = "neo4j.graphalgo.processors";
    private static final int MAX_CE_CONCURRENCY = 4;

    final int maxConcurrency;
    final int defaultConcurrency;

    ConcurrencyConfig() {
        this(Package.getPackage("org.neo4j.kernel.impl.enterprise") != null);
    }

    ConcurrencyConfig(boolean isOnEnterprise) {
        maxConcurrency = loadMaxConcurrency(isOnEnterprise);
        defaultConcurrency = loadDefaultConcurrency(isOnEnterprise);
    }

    static int loadMaxConcurrency(boolean isOnEnterprise) {
        return isOnEnterprise ? Integer.MAX_VALUE : MAX_CE_CONCURRENCY;
    }

    static int loadDefaultConcurrency(boolean isOnEnterprise) {
        Integer definedProcessors = null;
        try {
            definedProcessors = Integer.getInteger(PROCESSORS_OVERRIDE_PROPERTY);
        } catch (SecurityException ignored) {
        }
        if (definedProcessors == null) {
            definedProcessors = Runtime.getRuntime().availableProcessors();
        }
        if (!isOnEnterprise) {
            definedProcessors = Math.min(definedProcessors, MAX_CE_CONCURRENCY);
        }
        return definedProcessors;
    }
}
