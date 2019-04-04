package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcedureConfigurationTest {

    @Test
    public void useDefault() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void returnValueIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "partition");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("partition", value);
    }

    @Test
    public void newKeyIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old", "writeProperty", "new");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("new", value);
    }

    @Test
    public void oldKeyIfNewKeyNotPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("old", value);
    }

    @Test
    public void defaultIfNoKeysPresent() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void defaultIfKeyMissing() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void defaultIfKeyPresentButNoValue() {
        Map<String, Object> map = MapUtil.map("writeProperty", "");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void valueIfKeyPresent() {
        Map<String, Object> map = MapUtil.map("writeProperty", "scc");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("scc", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void limitConcurrencyOnCommunityEdition() {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 42));
        Dependencies deps = new Dependencies();
        deps.satisfyDependency(DatabaseInfo.COMMUNITY);

        assertEquals(Pools.MAX_CE_CONCURRENCY, configuration.getConcurrency(deps));
    }

    @Test
    public void limitDefaultConcurrencyOnCommunityEdition() {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map());
        Dependencies deps = new Dependencies();
        deps.satisfyDependency(DatabaseInfo.COMMUNITY);

        assertEquals(Pools.MAX_CE_CONCURRENCY, configuration.getConcurrency(1337, deps));
    }

    @Test
    public void unlimitedConcurrencyOnEnterpriseEdition() throws IOException {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 42));
        Dependencies deps = new Dependencies();
        deps.satisfyDependency(DatabaseInfo.ENTERPRISE);

        assertEquals(42, configuration.getConcurrency(deps));
    }

    @Test
    public void unlimitedDefaultConcurrencyOnEnterpriseEdition() throws IOException {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map());
        Dependencies deps = new Dependencies();
        deps.satisfyDependency(DatabaseInfo.ENTERPRISE);

        assertEquals(1337, configuration.getConcurrency(1337, deps));
    }
}
