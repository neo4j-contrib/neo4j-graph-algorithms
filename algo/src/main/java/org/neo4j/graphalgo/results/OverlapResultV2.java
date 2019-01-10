package org.neo4j.graphalgo.results;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.NavigableSet;

public class OverlapResultV2 {
    public HTreeMap<String, NavigableSet<Long>> partnerOverlappedLoads;
    public HTreeMap<String, NavigableSet<Long>> partnerOverlappedMyLoads;
    public HTreeMap<String, NavigableSet<Long>> partnerOverlappedLanes;
    public HTreeMap<String, NavigableSet<Long>> partnerOverlappedMyLanes;
    public HTreeMap<String, NavigableSet<String>> partnerOverlappedCarriers;
    public HTreeMap<String, NavigableSet<String>> partnerOverlappedMyCarriers;
    public HTreeMap<Long, NavigableSet<String>> lanePartners;
    public HTreeMap<Long, NavigableSet<String>> laneCarriers;
    public HTreeMap<Long, NavigableSet<Long>> lanePartnerLoads;
    public HTreeMap<Long, NavigableSet<Long>> laneParnterOverlaps;
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
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        laneCarriers = db
                .hashMap(tag + "laneCarriers")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        laneParnterOverlaps = db
                .hashMap(tag + "laneParnterOverlaps")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        lanePartnerLoads = db
                .hashMap(tag + "lanePartnerLoads")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        this.tag = tag;
    }
}
