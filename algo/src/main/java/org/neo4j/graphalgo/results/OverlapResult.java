package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class OverlapResult {
    public ObjectObjectMap<String, LongHashSet> partnerOverlappedLoads;
    public ObjectObjectMap<String, LongHashSet> partnerOverlappedMyLoads;
    public ObjectObjectMap<String, LongHashSet> partnerOverlappedLanes;
    public ObjectObjectMap<String, LongHashSet> partnerOverlappedMyLanes;
    public ObjectObjectMap<String, ObjectHashSet<String>> partnerOverlappedCarriers;
    public ObjectObjectMap<String, ObjectHashSet<String> > partnerOverlappedMyCarriers;
    public LongObjectMap<ObjectHashSet<String>> lanePartners;
    public LongObjectMap<ObjectHashSet<String>> laneCarriers;
    public LongObjectMap<LongHashSet> lanePartnerLoads;
    public LongObjectMap<LongHashSet> laneParnterOverlaps;
    public Map<String, JSONArray> partnerLanesJson;

    public OverlapResult(){
        partnerOverlappedLoads = new ObjectObjectHashMap<>();
        partnerOverlappedMyLoads = new ObjectObjectHashMap<>();
        partnerOverlappedLanes = new ObjectObjectHashMap<>();
        partnerOverlappedMyLanes = new ObjectObjectHashMap<>();
        partnerOverlappedCarriers = new ObjectObjectHashMap<>();
        partnerOverlappedMyCarriers = new ObjectObjectHashMap<>();
        lanePartners = new LongObjectHashMap<>();
        laneCarriers = new LongObjectHashMap<>();
        laneParnterOverlaps = new LongObjectHashMap<>();
        lanePartnerLoads = new LongObjectHashMap<>();
        partnerLanesJson = new HashMap<>();
    }
}
