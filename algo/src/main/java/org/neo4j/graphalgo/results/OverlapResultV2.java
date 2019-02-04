package org.neo4j.graphalgo.results;

import org.mapdb.DB;
import org.mapdb.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OverlapResultV2 {
    public Map<String, Set<Long>> partnerOverlappedLoads;
    public Map<String, Set<Long>> partnerOverlappedMyLoads;
    public Map<String, Set<Long>> partnerOverlappedLanes;
    public Map<String, Set<Long>> partnerOverlappedMyLanes;
    public Map<String, Set<String>> partnerOverlappedCarriers;
    public Map<String, Set<String>> partnerOverlappedMyCarriers;
    public Map<Long, Integer> lanePartners;
    public Map<Long, Integer> laneCarriers;
    public Map<Long, Integer> lanePartnerLoads;
    public Map<Long, Integer> lanePartnerOverlaps;
    public String tag;

    public OverlapResultV2(DB db, String tag){
//        partnerOverlappedLoads = db.hashMap(tag + "_" + "partnerOverlappedLoads")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        partnerOverlappedMyLoads = db.hashMap(tag + "_" + "partnerOverlappedMyLoads")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        partnerOverlappedLanes = db.hashMap(tag + "_" + "partnerOverlappedLanes")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        partnerOverlappedMyLanes = db.hashMap(tag + "_" + "partnerOverlappedMyLanes")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        partnerOverlappedCarriers = db.hashMap(tag + "_" + "partnerOverlappedCarriers")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        partnerOverlappedMyCarriers =  db.hashMap(tag + "_" + "partnerOverlappedMyCarriers")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        lanePartners = db.hashMap(tag + "_" + "lanePartners")
//                .keySerializer(Serializer.LONG)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        laneCarriers = db.hashMap(tag + "_" + "laneCarriers")
//                .keySerializer(Serializer.LONG)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        lanePartnerOverlaps = db.hashMap(tag + "_" + "lanePartnerOverlaps")
//                .keySerializer(Serializer.LONG)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();
//        lanePartnerLoads =  db.hashMap(tag + "_" + "lanePartnerLoads")
//                .keySerializer(Serializer.LONG)
//                .valueSerializer(Serializer.JAVA)
//                .createOrOpen();

        partnerOverlappedLoads = new HashMap<>();
        partnerOverlappedMyLoads = new HashMap<>();
        partnerOverlappedLanes = new HashMap<>();
        partnerOverlappedMyLanes = new HashMap<>();
        partnerOverlappedCarriers = new HashMap<>();
        partnerOverlappedMyCarriers =  new HashMap<>();
        lanePartners = new HashMap<>();
        laneCarriers = new HashMap<>();
        lanePartnerOverlaps = new HashMap<>();
        lanePartnerLoads =  new HashMap<>();
        this.tag = tag;
    }
}
