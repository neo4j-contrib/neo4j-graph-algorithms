package org.neo4j.graphalgo;

public class PropertyMapping {
    public final String type;
    public final String propertyKey;
    public final double defaultValue;

    public PropertyMapping(String type, String propertyKey, double defaultValue) {

        this.type = type;
        this.propertyKey = propertyKey;
        this.defaultValue = defaultValue;
    }

    public static PropertyMapping of(String type, String propertyKey, double defaultValue) {
        return new PropertyMapping(type, propertyKey, defaultValue);
    }


}
