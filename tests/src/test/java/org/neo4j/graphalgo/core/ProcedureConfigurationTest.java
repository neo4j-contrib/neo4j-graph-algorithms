package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class ProcedureConfigurationTest {

    @Test
    public void useDefault() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void returnValueIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "partition");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("partition", value);
    }

    @Test
    public void newKeyIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old", "writeProperty", "new");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("new", value);
    }

    @Test
    public void oldKeyIfNewKeyNotPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("old", value);
    }

    @Test
    public void defaultIfNoKeysPresent() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void defaultIfKeyMissing() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void defaultIfKeyPresentButNoValue() {
        Map<String, Object> map = MapUtil.map("writeProperty", "");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void valueIfKeyPresent() {
        Map<String, Object> map = MapUtil.map("writeProperty", "scc");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        assertEquals("scc", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }
}