package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Result;

import java.util.Map;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long rows;
    private IdMap idMap;
    private Map<PropertyMapping, WeightMap> nodeProperties;

    NodeRowVisitor(IdMap idMap, Map<PropertyMapping, WeightMap> nodeProperties) {
        this.idMap = idMap;
        this.nodeProperties = nodeProperties;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;
        long id = row.getNumber("id").longValue();
        idMap.add(id);

        for (Map.Entry<PropertyMapping, WeightMap> entry : nodeProperties.entrySet()) {
            Object value = CypherLoadingUtils.getProperty(row, entry.getKey().propertyKey);
            if (value instanceof Number) {
                entry.getValue().put(id, ((Number) value).doubleValue());
            }
        }

        return true;
    }

    long rows() {
        return rows;
    }
}
