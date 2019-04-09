package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ConcurrencyConfigTest {

    @Test
    public void limitConcurrencyOnCommunityEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 42, /* isOnEnterprise */ false);
        assertEquals(4, config.defaultConcurrency);
        assertEquals(4, config.maxConcurrency);
    }

    @Test
    public void allowLowerThanMaxSettingsOnCommunityEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 2, /* isOnEnterprise */ false);
        assertEquals(4, config.maxConcurrency);
        assertEquals(2, config.defaultConcurrency);
    }

    @Test
    public void unlimitedDefaultConcurrencyOnEnterpriseEdition() {
        ConcurrencyConfig config = new ConcurrencyConfig(/* cpus */ 42, /* isOnEnterprise */ true);
        assertEquals(42, config.defaultConcurrency);
        assertEquals(Integer.MAX_VALUE, config.maxConcurrency);
    }
}
