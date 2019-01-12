package org.neo4j.graphalgo.results;

import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.Set;

public class OverlapResultV2 {
    public HTreeMap<String, Set<Long>> partnerOverlappedLoads;
    public HTreeMap<String, Set<Long>> partnerOverlappedMyLoads;
    public HTreeMap<String, Set<Long>> partnerOverlappedLanes;
    public HTreeMap<String, Set<Long>> partnerOverlappedMyLanes;
    public HTreeMap<String, Set<String>> partnerOverlappedCarriers;
    public HTreeMap<String, Set<String>> partnerOverlappedMyCarriers;
    public HTreeMap<Long, Set<String>> lanePartners;
    public HTreeMap<Long, Set<String>> laneCarriers;
    public HTreeMap<Long, Set<Long>> lanePartnerLoads;
    public HTreeMap<Long, Set<Long>> laneParnterOverlaps;
    public String tag;

    public OverlapResultV2(DB db, String tag){
        partnerOverlappedLoads = db
                .hashMap(tag + "partnerOverlappedLoads")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        partnerOverlappedMyLoads = db
                .hashMap(tag + "partnerOverlappedMyLoads")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        partnerOverlappedLanes = db
                .hashMap(tag + "partnerOverlappedLanes")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        partnerOverlappedMyLanes = db
                .hashMap(tag + "partnerOverlappedMyLanes")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        partnerOverlappedCarriers =db
                .hashMap(tag + "partnerOverlappedCarriers")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        partnerOverlappedMyCarriers = db
                .hashMap(tag + "partnerOverlappedMyCarriers")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        lanePartners =db
                .hashMap(tag + "lanePartners")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        laneCarriers = db
                .hashMap(tag + "laneCarriers")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        laneParnterOverlaps = db
                .hashMap(tag + "laneParnterOverlaps")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        lanePartnerLoads = db
                .hashMap(tag + "lanePartnerLoads")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        this.tag = tag;
    }
}
