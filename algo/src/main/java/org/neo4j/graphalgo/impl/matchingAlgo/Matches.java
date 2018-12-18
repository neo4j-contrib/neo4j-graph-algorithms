package org.neo4j.graphalgo.impl.matchingAlgo;

public class Matches {
    private String Uuid;
    private int offRoute;
    private int deadHead;

    public Matches(String Uuid, int offRoute, int deadHead){
        this.Uuid = Uuid;
        this.offRoute = offRoute;
        this.deadHead = deadHead;
    }

    public String getUuid() {
        return Uuid;
    }

    public int getOffRoute() {
        return offRoute;
    }

    public int getDeadHead() {
        return deadHead;
    }
}
