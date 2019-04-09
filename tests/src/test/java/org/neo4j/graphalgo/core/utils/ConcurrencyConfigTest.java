package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ConcurrencyConfigTest {

    @Test
    public void limitConcurrencyOnCommunityEdition() {
        // just to make sure in case we run on an environment with less cores
        // set this to a value larger than the CE limitation
        System.setProperty("neo4j.graphalgo.processors", "8");
        ConcurrencyConfig config = new ConcurrencyConfig(/* isOnEnterprise */ false);
        assertEquals(4, config.defaultConcurrency);
        assertEquals(4, config.maxConcurrency);
    }

    @Test
    public void allowLowerThanMaxSettingsOnCommunityEdition() {
        System.setProperty("neo4j.graphalgo.processors", "2");
        ConcurrencyConfig config = new ConcurrencyConfig(/* isOnEnterprise */ false);
        assertEquals(4, config.maxConcurrency);
        assertEquals(2, config.defaultConcurrency);
    }

    @Test
    public void unlimitedDefaultConcurrencyOnEnterpriseEdition() {
        // set fixed value that we will assert on
        System.setProperty("neo4j.graphalgo.processors", "42");
        ConcurrencyConfig config = new ConcurrencyConfig(/* isOnEnterprise */ true);
        assertEquals(42, config.defaultConcurrency);
        assertEquals(Integer.MAX_VALUE, config.maxConcurrency);
    }
}
