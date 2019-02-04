package org.neo4j.graphalgo.impl;

import au.com.bytecode.opencsv.CSVWriter;
import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.results.OverlapResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;

public class AnalysisAlgo extends Algorithm<AnalysisAlgo> {
    private final GraphDatabaseAPI dbService;
    private final Log log;

    private String companyId;
    private String directory;
    private String filename;

    private String loadLaneRelName;
    private String laneStartName;
    private String laneEndName;

    private double deadHeadPercentage;
    private double offRoutePercentage;

    private int travelDirectionDeviation;

    private ResourceIterator<Node> loads;

    private LongLongMap loadLaneMap;
    private LongObjectMap<Node> loadMap;

    private LongObjectMap<LongScatterSet> laneLoads;
    private LongObjectMap<ObjectHashSet<String>> laneCarriers;
    private LongObjectMap<Node> laneMap;

    private LongObjectMap<LongScatterSet> sameSideOverlap;
    private LongObjectMap<LongScatterSet> circularOverlap;
    private LongObjectMap<LongScatterSet> continuousRunOverlap;

    private OverlapResult sameSideOverlapResult;
    private OverlapResult circularOverlapResult;
    private OverlapResult continuousRunOverlapResult;

    private ObjectHashSet<String> partnersList;

    private Set<String> originCities;

    private Set<String> destinationCities;

//    private LongObjectMap<Node> overlapLaneMap;

    private ObjectObjectMap<String, String> partnerMaskedMap;

    private String partnerMaskedMapLoadFile;
    private String carrierMaskedMapLoadFile;

    private Long limit = null;


    private ObjectIntMap<String> partnerCarrierCount;

    private ObjectObjectMap<String, String> carrierMaskedMap;


    private int partnerCount;

    private boolean maskPartners;

    public AnalysisAlgo(final String companyId, final String directory, final ProcedureConfiguration configuration, final GraphDatabaseAPI dbService, final Log log) {
        this.companyId = companyId;
        this.directory = directory;
        this.filename = directory + companyId;
        this.dbService = dbService;
        this.log = log;
        this.loadLaneRelName = configuration.get("loadLaneRelName", "LOAD_LANE");
        this.laneStartName = configuration.get("laneStartName", "LANE_START_CITY");
        this.laneEndName = configuration.get("laneEndName", "LANE_END_CITY");
        this.offRoutePercentage = configuration.get("offRoutePercentage", 1.50);
        this.deadHeadPercentage = configuration.get("deadHeadPercentage", 0.50);
        this.travelDirectionDeviation = configuration.get("travelDirectionDeviation", 60);
        this.partnerMaskedMapLoadFile = configuration.get("partnerMaskedMapLoadFile", null);
        this.carrierMaskedMapLoadFile = configuration.get("carrierMaskedMapLoadFile", null);
        this.limit = configuration.get("limit", null);
        this.maskPartners = configuration.get("maskPartners", true);
    }

    public void initialize(){
        loadLaneMap = new LongLongHashMap();
        loadMap = new LongObjectHashMap<>();

        laneLoads = new LongObjectScatterMap<>();
        laneCarriers = new LongObjectHashMap<>();
        laneMap = new LongObjectScatterMap<>();

        sameSideOverlap = new LongObjectHashMap<>();
        circularOverlap = new LongObjectHashMap<>();
        continuousRunOverlap = new LongObjectHashMap<>();

//        overlapLaneMap = new LongObjectScatterMap<>();

        partnerMaskedMap = new ObjectObjectHashMap<>();

        partnerCarrierCount = new ObjectIntHashMap<>();
        carrierMaskedMap = new ObjectObjectHashMap<>();

        sameSideOverlapResult = new OverlapResult();
        circularOverlapResult = new OverlapResult();
        continuousRunOverlapResult = new OverlapResult();

        partnersList = new ObjectHashSet<>();

        originCities = new HashSet<>();
        destinationCities = new HashSet<>();
    }

    private void loadLanes(){
        long startTime;
        long endTime;
        startTime = System.nanoTime();
        loads =  dbService.findNodes(Label.label("Load"), "shipper_id", companyId);
        loads.stream().forEach(load -> {
            loadMap.put(load.getId(), load);
            if(!load.hasRelationship(RelationshipType.withName(loadLaneRelName), Direction.OUTGOING)){
                return;
            }
            Node lane = load.getSingleRelationship(RelationshipType.withName(loadLaneRelName), Direction.OUTGOING).getEndNode();
            loadLaneMap.put(load.getId(), lane.getId());
            laneMap.put(lane.getId(), lane);
            LongScatterSet loadIds = laneLoads.get(lane.getId());
            if( loadIds == null ){
                loadIds = new LongScatterSet();
                laneLoads.put(lane.getId(), loadIds);
            }
            loadIds.add(load.getId());
            ObjectHashSet<String> carriers = laneCarriers.get(lane.getId());
            if( carriers == null ){
                carriers = new ObjectHashSet<>();
                laneCarriers.put(lane.getId(), carriers);
            }
            carriers.add((String) load.getProperty("carrier_id"));
        });
        endTime = System.nanoTime();
        loads.close();
        log.info("Completed Load Get and Lane Create " + (endTime - startTime) + " Size " + loadMap.size());
    }

    private void calculateLaneOverlaps(){
        long startTime;
        long endTime;
        int index = 0;
        for (LongObjectCursor<LongScatterSet> laneLoad : laneLoads) {
            TraversalDescription routeTd = dbService.traversalDescription()
                    .breadthFirst()
                    .relationships(RelationshipType.withName("ROUTE"), Direction.BOTH)
                    .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                    .evaluator(Evaluators.atDepth(1));
            index += 1;
            Long laneId = laneLoad.key;
            Node lane = laneMap.get(laneId);
            Node lpc = lane.getSingleRelationship(RelationshipType.withName(laneStartName), Direction.OUTGOING).getEndNode();
            Node ldc = lane.getSingleRelationship(RelationshipType.withName(laneEndName), Direction.OUTGOING).getEndNode();
            double laneDistance = (Double) lane.getProperty("distance");
            double travelDirection;
            double reverseTravelDirection;
            startTime = System.nanoTime();
            log.info("Inside lane Calculation: " + index + " out of " + laneLoads.size());
            if (lane.hasProperty("travel_direction")) {
                travelDirection = (double) lane.getProperty("travel_direction");
            } else {
                travelDirection = bearing((double) lane.getProperty("start_city_latitude"),
                        (double) lane.getProperty("start_city_longitude"),
                        (double) lane.getProperty("end_city_latitude"),
                        (double) lane.getProperty("end_city_longitude"
                        ));
                lane.setProperty("travel_direction", travelDirection);
            }
            if (lane.hasProperty("reverse_travel_direction")) {
                reverseTravelDirection = (double) lane.getProperty("reverse_travel_direction");
            } else {
                reverseTravelDirection = bearing((double) lane.getProperty("end_city_latitude"),
                        (double) lane.getProperty("end_city_longitude"),
                        (double) lane.getProperty("start_city_latitude"),
                        (double) lane.getProperty("start_city_longitude"
                        ));
                lane.setProperty("reverse_travel_direction", reverseTravelDirection);
            }
            Double firstArchStart = null;
            Double firstArchEnd = null;
            Double secondArchStart = null;
            Double secondArchEnd = null;

            Double rfirstArchStart = null;
            Double rfirstArchEnd = null;
            Double rsecondArchStart = null;
            Double rsecondArchEnd = null;

            if (travelDirection < travelDirectionDeviation ) {
                firstArchStart = 0.0;
                firstArchEnd = travelDirection + travelDirectionDeviation;
                secondArchStart = 360.0 - Math.abs(travelDirection - travelDirectionDeviation);
                secondArchEnd = 360.0;
            } else if (travelDirection > 300) {
                firstArchStart = travelDirection - travelDirectionDeviation;
                firstArchEnd = 360.0;
                secondArchStart = 0.0;
                secondArchEnd = (travelDirection + 60) % 360.0;
            } else {
                firstArchStart = travelDirection - 60.0;
                firstArchEnd = travelDirection + 60.0;
            }

            if (reverseTravelDirection < travelDirectionDeviation ) {
                rfirstArchStart = 0.0;
                rfirstArchEnd = reverseTravelDirection + travelDirectionDeviation;
                rsecondArchStart = 360.0 - Math.abs(reverseTravelDirection - travelDirectionDeviation);
                rsecondArchEnd = 360.0;
            } else if (reverseTravelDirection > 300) {
                rfirstArchStart = reverseTravelDirection - travelDirectionDeviation;
                rfirstArchEnd = 360.0;
                rsecondArchStart = 0.0;
                rsecondArchEnd = (reverseTravelDirection + 60) % 360.0;
            } else {
                rfirstArchStart = reverseTravelDirection - 60.0;
                rfirstArchEnd = reverseTravelDirection + 60.0;
            }

            Traverser lpcRouteTraverse = routeTd.traverse(lpc);
            for (Path routePath : lpcRouteTraverse) {
                Relationship cocRel = routePath.lastRelationship();
                double pathDistance = (double) cocRel.getProperty("distance");
                if (pathDistance > laneDistance * deadHeadPercentage) {
                    continue;
                }
                Node coc = cocRel.getOtherNode(lpc);
                Iterable<Relationship> cocLaneRels = coc.getRelationships(RelationshipType.withName(laneStartName), Direction.INCOMING);
                for (Relationship cocLaneRel : cocLaneRels) {
                    Node possibleOverlapLane = cocLaneRel.getStartNode();
                    if (pathDistance + (double) possibleOverlapLane.getProperty("distance") > laneDistance * offRoutePercentage) {
                        continue;
                    }
//                    overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
                    double overlapTravelDirection;
                    if (possibleOverlapLane.hasProperty("travel_direction")) {
                        overlapTravelDirection = (double) possibleOverlapLane.getProperty("travel_direction");
                    } else {
                        overlapTravelDirection = bearing((double) possibleOverlapLane.getProperty("start_city_latitude"),
                                (double) possibleOverlapLane.getProperty("start_city_longitude"),
                                (double) possibleOverlapLane.getProperty("end_city_latitude"),
                                (double) possibleOverlapLane.getProperty("end_city_longitude"
                                ));
                        possibleOverlapLane.setProperty("travel_direction", overlapTravelDirection);
                    }

                    if (possibleOverlapLane.hasProperty("travel_direction")) {
                        overlapTravelDirection = (double) possibleOverlapLane.getProperty("travel_direction");
                    } else {
                        overlapTravelDirection = bearing((double) possibleOverlapLane.getProperty("start_city_latitude"),
                                (double) possibleOverlapLane.getProperty("start_city_longitude"),
                                (double) possibleOverlapLane.getProperty("end_city_latitude"),
                                (double) possibleOverlapLane.getProperty("end_city_longitude"
                                ));
                        possibleOverlapLane.setProperty("travel_direction", overlapTravelDirection);
                    }
                    if(secondArchEnd != null){
                        if( (overlapTravelDirection > secondArchStart && overlapTravelDirection < secondArchEnd) || (overlapTravelDirection > firstArchStart && overlapTravelDirection < firstArchEnd ) )
                        {
                            LongScatterSet sameSideOverlaps = sameSideOverlap.get(lane.getId());
                            if (null == sameSideOverlaps) {
                                sameSideOverlaps = new LongScatterSet();
                                sameSideOverlap.put(lane.getId(), sameSideOverlaps);
                            }
                            sameSideOverlaps.add(possibleOverlapLane.getId());
                        }
                    }
                    else {
                        if ( overlapTravelDirection > firstArchStart && overlapTravelDirection < firstArchEnd ) {
                            LongScatterSet sameSideOverlaps = sameSideOverlap.get(lane.getId());
                            if (null == sameSideOverlaps) {
                                sameSideOverlaps = new LongScatterSet();
                                sameSideOverlap.put(lane.getId(), sameSideOverlaps);
                            }
                            sameSideOverlaps.add(possibleOverlapLane.getId());
                        }
                    }
                }
            }
            Traverser ldcRouteTraverse = routeTd.traverse(ldc);
            for (Path routePath : ldcRouteTraverse) {

                Relationship cocRel = routePath.lastRelationship();
                double pathDistance = (double) cocRel.getProperty("distance");
                if (pathDistance > laneDistance * deadHeadPercentage) {
                    continue;
                }
                Node coc = cocRel.getOtherNode(ldc);
                Iterable<Relationship> cocLaneRels = coc.getRelationships(RelationshipType.withName(laneStartName), Direction.INCOMING);
                for (Relationship cocLaneRel : cocLaneRels) {
                    Node possibleOverlapLane = cocLaneRel.getStartNode();
                    if (pathDistance + (double) possibleOverlapLane.getProperty("distance") > laneDistance * offRoutePercentage) {
                        continue;
                    }
//                    overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
                    double overlapTravelDirection;
                    if (possibleOverlapLane.hasProperty("travel_direction")) {
                        overlapTravelDirection = (double) possibleOverlapLane.getProperty("travel_direction");
                    } else {
                        overlapTravelDirection = bearing((double) possibleOverlapLane.getProperty("start_city_latitude"),
                                (double) possibleOverlapLane.getProperty("start_city_longitude"),
                                (double) possibleOverlapLane.getProperty("end_city_latitude"),
                                (double) possibleOverlapLane.getProperty("end_city_longitude"
                                ));
                        possibleOverlapLane.setProperty("travel_direction", overlapTravelDirection);
                    }

                    if(rsecondArchEnd != null){
                        if( (overlapTravelDirection > rsecondArchStart && overlapTravelDirection < rsecondArchEnd) || (overlapTravelDirection > rfirstArchStart && overlapTravelDirection < rfirstArchEnd) ) {
                            LongScatterSet circularOverlaps = circularOverlap.get(lane.getId());
                            if (null == circularOverlaps) {
                                circularOverlaps = new LongScatterSet();
                                circularOverlap.put(lane.getId(), circularOverlaps);
                            }
                            circularOverlaps.add(possibleOverlapLane.getId());
                        }
                        else {
                            LongScatterSet continuousRunOverlaps = continuousRunOverlap.get(lane.getId());
                            if (null == continuousRunOverlaps) {
                                continuousRunOverlaps = new LongScatterSet();
                                continuousRunOverlap.put(lane.getId(), continuousRunOverlaps);
                            }
                            continuousRunOverlaps.add(possibleOverlapLane.getId());
                        }
                    }
                    else {
                        if ( overlapTravelDirection > rfirstArchStart && overlapTravelDirection < rfirstArchEnd ) {
                            LongScatterSet circularOverlaps = circularOverlap.get(lane.getId());
                            if (null == circularOverlaps) {
                                circularOverlaps = new LongScatterSet();
                                circularOverlap.put(lane.getId(), circularOverlaps);
                            }
                            circularOverlaps.add(possibleOverlapLane.getId());                        }
                        else{
                            LongScatterSet continuousRunOverlaps = continuousRunOverlap.get(lane.getId());
                            if (null == continuousRunOverlaps) {
                                continuousRunOverlaps = new LongScatterSet();
                                continuousRunOverlap.put(lane.getId(), continuousRunOverlaps);
                            }
                            continuousRunOverlaps.add(possibleOverlapLane.getId());                        }
                    }
                }
            }
            endTime = System.nanoTime();
            log.info("Completed lane Calculation: " + index + " out of " + laneLoads.size() + " in " + (endTime - startTime) + " ns");
            if(limit != null){
                if(limit == index){
                    break;
                }
            }
        }
    }

    private String getMaskedParnter(String partner){
        if(!maskPartners) {
           return partner;
        }

        if(partnerMaskedMap.containsKey(partner)){
            return partnerMaskedMap.get(partner);
        }

        partnerCount++;
        String maskedPartner = String.format("partner%s", Integer.toString(partnerCount));


        return maskedPartner;
    }

    private String getMaskedCarrier(String partner, String carrier){
        String partnerCarrier = String.format("%s|%s", partner, carrier);
        if(!maskPartners) {
            return partnerCarrier;
        }
        if(carrierMaskedMap.containsKey(partnerCarrier)){
            return carrierMaskedMap.get(partnerCarrier);
        }

        int carrierCount = 0;
        if(partnerCarrierCount.containsKey(partner)){
            carrierCount = partnerCarrierCount.get(partner);
        }
        carrierCount++;
        partnerCarrierCount.put(partner, carrierCount);
        String maskedPartner = getMaskedParnter(partner);
        String maskedCarrier = String.format("%s|carrier%s", maskedPartner, Integer.toString(carrierCount));
        return maskedCarrier;
    }

    private void printMaskedPartnerFile(){
        String maskedPartnerfilename = String.format("%s_masked_partners.csv", filename);
        File maskedPartnerfile = new File(maskedPartnerfilename);
        try {
            FileWriter fileOutput = new FileWriter(maskedPartnerfile);
            CSVWriter writer = new CSVWriter(fileOutput);
            String[] header = {"partner", "masked_name"};
            writer.writeNext(header);
            Iterator<ObjectObjectCursor<String, String>> partnerMaskedMapIterator = partnerMaskedMap.iterator();
            while (partnerMaskedMapIterator.hasNext()) {
                String[] data = new String[2];
                int row = 0;
                ObjectObjectCursor<String, String> o = partnerMaskedMapIterator.next();
                data[row++] = o.key;
                data[row++] = o.value;
                writer.writeNext(data);
            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
    }

    private void printMaskedCarrierFile(){
        String maskedCarrierfilename = String.format("%s_masked_carriers.csv", filename);
        File maskedCarrierfile = new File(maskedCarrierfilename);
        try {
            FileWriter fileOutput = new FileWriter(maskedCarrierfile);
            CSVWriter writer = new CSVWriter(fileOutput);
            String[] header = {"carrier", "masked_name"};
            writer.writeNext(header);
            Iterator<ObjectObjectCursor<String, String>> carrierMaskedMapIterator = carrierMaskedMap.iterator();
            while (carrierMaskedMapIterator.hasNext()) {
                String[] data = new String[2];
                int row = 0;
                ObjectObjectCursor<String, String> o = carrierMaskedMapIterator.next();
                data[row++] = o.key;
                data[row++] = o.value;
                writer.writeNext(data);
            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
    }

    private  void printLaneSummaryFile(){
        File laneSummaryfile = new File(String.format("%s_lanes_summary.csv", filename));
        try {
            FileWriter fileOutput = new FileWriter(laneSummaryfile);
            CSVWriter writer = new CSVWriter(fileOutput);
            String[] header = {"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state",
                    "my_loads", "my_carriers",
                    "same_side_partners_count", "same_side_partner_overlaps_count",
                    "same_side_partner_loads_count", "same_side_partner_carriers_count",
                    "circular_partner_count", "circular_partner_overlaps_count",
                    "circular_partner_loads_count", "circular_partner_carriers_count",
                    "continuous_run_partner_count", "continuous_run_partner_overlaps_count",
                    "continuous_run_partner_loads_count", "continuous_run_partner_carriers_count"};
            writer.writeNext(header);

            for (LongObjectCursor<Node> laneCursor : laneMap) {
                Node lane = laneCursor.value;
                long laneId = lane.getId();
                String[] data = new String[header.length];
                int row = 0;
                data[row++] = (String) lane.getProperty("start_city");
                data[row++] = (String) lane.getProperty("start_state");
                data[row++] = (String) lane.getProperty("end_city");
                data[row++] = (String) lane.getProperty("end_state");
                data[row++] = String.valueOf(laneLoads.get(laneId).size());
                data[row++] = String.valueOf(laneCarriers.get(laneId).size());
                data[row++] = sameSideOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.lanePartners.get(laneId).size()) : "0";
                data[row++] = sameSideOverlapResult.laneParnterOverlaps.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.laneParnterOverlaps.get(laneId).size()) : "0" ;
                data[row++] = sameSideOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.lanePartnerLoads.get(laneId).size()) : "0" ;
                data[row++] = sameSideOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.laneCarriers.get(laneId).size()) : "0" ;
                data[row++] = circularOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(circularOverlapResult.lanePartners.get(laneId).size()) : "0" ;
                data[row++] = circularOverlapResult.laneParnterOverlaps.containsKey(laneId) ? String.valueOf(circularOverlapResult.laneParnterOverlaps.get(laneId).size()) : "0" ;
                data[row++] = circularOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(circularOverlapResult.lanePartnerLoads.get(laneId).size()) : "0" ;
                data[row++] = circularOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(circularOverlapResult.laneCarriers.get(laneId).size()) : "0" ;
                data[row++] = continuousRunOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.lanePartners.get(laneId).size()) : "0" ;
                data[row++] = continuousRunOverlapResult.laneParnterOverlaps.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.laneParnterOverlaps.get(laneId).size()) : "0" ;
                data[row++] = continuousRunOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.lanePartnerLoads.get(laneId).size()) : "0" ;
                data[row++] = continuousRunOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.laneCarriers.get(laneId).size()) : "0" ;

                writer.writeNext(data);
            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
    }

    private void printResultsToFile(String filename, LongObjectMap<LongScatterSet> Overlap, OverlapResult overlapResult , boolean isLaneOverlapReport, boolean isSummary, String[] header){
        //results Printer
        File laneSummaryfile = new File(filename);
        try {
            FileWriter fileOutput = new FileWriter(laneSummaryfile);
            CSVWriter writer = new CSVWriter(fileOutput);
            writer.writeNext(header);

            for (LongObjectCursor<Node> laneCursor : laneMap) {
                Node lane = laneCursor.value;
                long laneId = lane.getId();
                String[] data = new String[header.length];
                int row = 0;
                data[row++] = (String) lane.getProperty("start_city");
                data[row++] = (String) lane.getProperty("start_state");
                data[row++] = (String) lane.getProperty("end_city");
                data[row++] = (String) lane.getProperty("end_state");
                if(Overlap.containsKey(laneId)){
                    LongScatterSet overlaps = Overlap.get(laneId);
                    ObjectObjectMap<String, LongHashSet> partnerLoadMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> partnerOverlapMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, ObjectHashSet<String>> partnerCarrierMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> carrierLoadMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> carrierLaneMap = new ObjectObjectHashMap<>();
                    for (LongCursor overlap : overlaps) {
                        long overlapLaneId = overlap.value;
                        Node overlapLane = dbService.getNodeById(overlapLaneId);
                        if (!overlapLane.hasRelationship(RelationshipType.withName(loadLaneRelName), Direction.INCOMING)) {
                            overlaps.remove(overlapLaneId);
                            continue;
                        }
                        if(isLaneOverlapReport){
                            String[] writeData = data.clone();
                            int innerRow = row;
                            writeData[innerRow++] = (String) overlapLane.getProperty("start_city");
                            writeData[innerRow++] = (String) overlapLane.getProperty("start_state");
                            writeData[innerRow++] = (String) overlapLane.getProperty("end_city");
                            writeData[innerRow++] = (String) overlapLane.getProperty("end_state");
                            if(overlapLane.hasRelationship(RelationshipType.withName("LANE_HAS_SHIPPER"), Direction.OUTGOING)){
                                Iterable<Relationship> partnerOverlapRels = overlapLane.getRelationships(RelationshipType.withName("LANE_HAS_SHIPPER"), Direction.OUTGOING);
                                ObjectHashSet<String> uniqueOverlappedPartners = new ObjectHashSet<>();
                                for (Relationship partnerOverlapRel : partnerOverlapRels) {
                                    Node shipperNode = partnerOverlapRel.getEndNode();
                                    if(shipperNode.hasProperty("permalink")){
                                        uniqueOverlappedPartners.add( (String) shipperNode.getProperty("permalink") );
                                    }
                                }
                                Iterator<ObjectCursor<String>> partnerIterator = uniqueOverlappedPartners.iterator();
                                while (partnerIterator.hasNext()){
                                    String[] writeData1 = writeData.clone();
                                    int innerRow1 = innerRow;
                                    String partnerName = partnerIterator.next().value;
                                    if(laneId == overlapLaneId && companyId.equalsIgnoreCase(partnerName)){
                                        continue;
                                    }
                                    writeData1[innerRow1++] = getMaskedParnter(partnerName);
                                    writer.writeNext(writeData1);
                                }
                            }
                            continue;
                        }
                        for (Relationship loadRel : overlapLane.getRelationships(RelationshipType.withName(loadLaneRelName), Direction.INCOMING)) {
                            Node partnerLoad = loadRel.getStartNode();
                            String partnerId = (String) partnerLoad.getProperty("shipper_id");
                            String partnerCarrierId = (String) partnerLoad.getProperty("carrier_id");
                            if(laneId == overlapLaneId && companyId.equalsIgnoreCase(partnerId)){
                                continue;
                            }
                            LongHashSet partnerLoadIds = partnerLoadMap.get(partnerId);
                            if (partnerLoadIds == null) {
                                partnerLoadIds = new LongHashSet();
                                partnerLoadMap.put(partnerId, partnerLoadIds);
                            }
                            partnerLoadIds.add(partnerLoad.getId());
                            ObjectHashSet<String> otherCarrierIds = partnerCarrierMap.get(partnerId);
                            if (otherCarrierIds == null) {
                                otherCarrierIds = new ObjectHashSet<>();
                                partnerCarrierMap.put(partnerId, otherCarrierIds);
                            }
                            otherCarrierIds.add(partnerCarrierId);
                            LongHashSet shipperOverlapIds = partnerOverlapMap.get(partnerId);
                            if(shipperOverlapIds == null){
                             shipperOverlapIds = new LongHashSet();
                             partnerOverlapMap.put(partnerId, shipperOverlapIds);
                            }
                            shipperOverlapIds.add(overlapLaneId);
                            LongHashSet carrierLoadIds = carrierLoadMap.get(partnerCarrierId);
                            if(carrierLoadIds == null){
                                carrierLoadIds = new LongHashSet();
                                carrierLoadMap.put(partnerCarrierId, carrierLoadIds);
                            }
                            carrierLoadIds.add(partnerLoad.getId());
                            LongHashSet carrierLaneIds = carrierLaneMap.get(partnerCarrierId);
                            if(carrierLaneIds == null){
                                carrierLaneIds = new LongHashSet();
                                carrierLaneMap.put(partnerCarrierId, carrierLaneIds);
                            }
                            carrierLaneIds.add(overlapLaneId);


                            //For Summary Report

                            LongHashSet partnerOverlappedLoadIds = overlapResult.partnerOverlappedLoads.get(partnerId);
                            if(partnerOverlappedLoadIds == null){
                                partnerOverlappedLoadIds = new LongHashSet();
                                overlapResult.partnerOverlappedLoads.put(partnerId, partnerOverlappedLoadIds);
                            }
                            partnerOverlappedLoadIds.add(partnerLoad.getId());

                            LongHashSet partnerOverlappedLaneIds = overlapResult.partnerOverlappedLanes.get(partnerId);
                            if(partnerOverlappedLaneIds == null){
                                partnerOverlappedLaneIds = new LongHashSet();
                                overlapResult.partnerOverlappedLanes.put(partnerId, partnerOverlappedLaneIds);
                            }
                            partnerOverlappedLaneIds.add(overlapLaneId);

                            ObjectHashSet<String> partnerOverlappedCarrierIds = overlapResult.partnerOverlappedCarriers.get(partnerId);
                            if(partnerOverlappedCarrierIds == null){
                                partnerOverlappedCarrierIds = new ObjectHashSet<>();
                                overlapResult.partnerOverlappedCarriers.put(partnerId, partnerOverlappedCarrierIds);
                            }
                            partnerOverlappedCarrierIds.add(partnerCarrierId);

                            LongHashSet shipperOverlappedLaneIds = overlapResult.partnerOverlappedMyLanes.get(partnerId);
                            if(shipperOverlappedLaneIds == null){
                                shipperOverlappedLaneIds = new LongHashSet();
                                overlapResult.partnerOverlappedMyLanes.put(partnerId, shipperOverlappedLaneIds);
                            }
                            shipperOverlappedLaneIds.add(laneId);

                            LongHashSet shipperOverlappedLoadIds = overlapResult.partnerOverlappedMyLoads.get(partnerId);
                            if(shipperOverlappedLoadIds == null){
                                shipperOverlappedLoadIds = new LongHashSet();
                                overlapResult.partnerOverlappedMyLoads.put(partnerId, shipperOverlappedLoadIds);
                            }
                            shipperOverlappedLoadIds.addAll(laneLoads.get(laneId));

                            ObjectHashSet<String> shipperOverlappedCarrierIds = overlapResult.partnerOverlappedMyCarriers.get(partnerId);
                            if(shipperOverlappedCarrierIds == null){
                                shipperOverlappedCarrierIds = new ObjectHashSet<>();
                                overlapResult.partnerOverlappedMyCarriers.put(partnerId, shipperOverlappedCarrierIds);
                            }
                            shipperOverlappedCarrierIds.addAll(laneCarriers.get(laneId));
                            partnersList.add(partnerId);

                            // for lane Summary Report
                            LongHashSet summaryPartnerLoadIds = overlapResult.lanePartnerLoads.get(laneId);
                            if (summaryPartnerLoadIds == null) {
                                summaryPartnerLoadIds = new LongHashSet();
                                overlapResult.lanePartnerLoads.put(laneId, summaryPartnerLoadIds);
                            }
                            summaryPartnerLoadIds.add(partnerLoad.getId());

                            LongHashSet laneParnterOverlapsIds = overlapResult.laneParnterOverlaps.get(laneId);
                            if(laneParnterOverlapsIds == null){
                                laneParnterOverlapsIds = new LongHashSet();
                                overlapResult.laneParnterOverlaps.put(laneId, laneParnterOverlapsIds);
                            }
                            laneParnterOverlapsIds.add(overlapLaneId);

                            ObjectHashSet<String> laneCarriersIds = overlapResult.laneCarriers.get(laneId);
                            if(laneCarriersIds == null){
                                laneCarriersIds = new ObjectHashSet<>();
                                overlapResult.laneCarriers.put(laneId, laneCarriersIds);
                            }
                            laneCarriersIds.add(partnerCarrierId);

                            ObjectHashSet<String> lanePartnerIds = overlapResult.lanePartners.get(laneId);
                            if(lanePartnerIds == null){
                                lanePartnerIds = new ObjectHashSet<>();
                                overlapResult.lanePartners.put(laneId, lanePartnerIds);
                            }
                            lanePartnerIds.add(partnerId);
                        }
                    }
                    for (ObjectObjectCursor<String, LongHashSet> partnerCursor : partnerLoadMap) {
                        String partner = partnerCursor.key;
                        String[] writeData = data.clone();
                        int innerRow = row;
                        //if (companyId.equalsIgnoreCase(partner)) continue;
                        writeData[innerRow++] = companyId;
                        writeData[innerRow++] = Integer.toString(laneLoads.get(laneId).size());
                        writeData[innerRow++] = Integer.toString(laneCarriers.get(laneId).size());
                        writeData[innerRow++] = getMaskedParnter(partner);
                        if (isSummary) {
                            writeData[innerRow++] = Integer.toString(partnerOverlapMap.get(partner).size());
                            writeData[innerRow++] = Integer.toString(partnerLoadMap.get(partner).size());
                            writeData[innerRow++] = partnerCarrierMap.containsKey(partner) ? Integer.toString(partnerCarrierMap.get(partner).size()) : "0";
                            writer.writeNext(writeData);
                        } else {
                            if (partnerCarrierMap.containsKey(partner)) {
                                for (ObjectCursor<String> partnerCarrierCursor : partnerCarrierMap.get(partner)) {
                                    String[] completeData = writeData.clone();
                                    int completeRow = innerRow;
                                    String carrierName = partnerCarrierCursor.value;
                                    completeData[completeRow++] = getMaskedCarrier(partner, carrierName);
                                    completeData[completeRow++] = Integer.toString(carrierLaneMap.get(carrierName).size());
                                    completeData[completeRow++] = Integer.toString(carrierLoadMap.get(carrierName).size());
                                    writer.writeNext(completeData);
                                }
                            } else {
                                writeData[innerRow++] = "0";
                                writeData[innerRow++] = "0";
                                writeData[innerRow++] = "0";
                                writer.writeNext(writeData);
                            }
                        }
                    }
                    for (ObjectObjectCursor<String, LongHashSet> partnerOverlapCursor : partnerOverlapMap){
                        String partnerId = partnerOverlapCursor.key;
                        JSONArray lanes = overlapResult.partnerLanesJson.get(partnerId);
                        if(lanes == null){
                            lanes = new JSONArray();
                        }
                        JSONObject laneObject = new JSONObject();
                        laneObject.put("laneId", laneId);
                        laneObject.put("start_city_name", lane.getProperty("start_city"));
                        laneObject.put("start_city_state", lane.getProperty("start_state"));
                        laneObject.put("end_city_name", lane.getProperty("end_city"));
                        laneObject.put("end_city_state", lane.getProperty("end_state"));
                        laneObject.put("start_city_latitude", lane.getProperty("start_city_latitude"));
                        laneObject.put("start_city_longitude", lane.getProperty("start_city_longitude"));
                        laneObject.put("end_city_latitude", lane.getProperty("end_city_latitude"));
                        laneObject.put("end_city_longitude", lane.getProperty("end_city_longitude"));
                        laneObject.put("my_loads_count", laneLoads.get(laneId).size());
                        laneObject.put("partner_loads_count", partnerLoadMap.get(partnerId).size());
                        laneObject.put("my_carrier_count", laneCarriers.get(laneId).size());
                        laneObject.put("partner_carrier_count", partnerCarrierMap.get(partnerId).size());
                        JSONArray laneOverlapsJson = new JSONArray();
                        for (LongCursor laneOverlapCursor : partnerOverlapCursor.value)
                        {
                            laneOverlapsJson.add(laneOverlapCursor.value);
                        }
                        laneObject.put("partner_overlaps", laneOverlapsJson);
                        laneObject.put("partner_overlaps_count", partnerOverlapCursor.value.size());
                        lanes.add(laneObject);
                        overlapResult.partnerLanesJson.put(partnerId, lanes);
                    }
                }

            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
        finally {
        }
    }

    private void printSummaryFile(){
        String summaryFilename = String.format("%s_summary_all_partners.csv", filename);
        File summaryFile = new File(summaryFilename);
        try {
            FileWriter fileOutput = new FileWriter(summaryFile);
            CSVWriter writer = new CSVWriter(fileOutput);
            String[] header = {"partner", 
                    "same_side_my_overlaps_count", "same_side_partner_overlaps_count",
                    "same_side_my_loads_count", "same_side_partner_loads_count", 
                    "same_side_my_carriers_count", "same_side_partner_carriers_count",
                    "circular_my_overlaps_count", "circular_partner_overlaps_count",
                    "circular_my_loads_count", "circular_partner_loads_count",
                    "circular_my_carriers_count", "circular_partner_carriers_count",
                    "continuous_run_my_overlaps_count", "continuous_run_partner_overlaps_count",
                    "continuous_run_my_loads_count", "continuous_run_partner_loads_count",
                    "continuous_run_my_carriers_count", "continuous_run_partner_carriers_count"};
            writer.writeNext(header);
            Iterator<ObjectCursor<String>> partnersIterator = partnersList.iterator();
            while(partnersIterator.hasNext())
            {
                String partnerId = partnersIterator.next().value;
                String[] data = new String[header.length];
                int row = 0;
                data[row++] = getMaskedParnter(partnerId);
                data[row++] = sameSideOverlapResult.partnerOverlappedMyLanes.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedMyLanes.get(partnerId).size()) : "0";
                data[row++] = sameSideOverlapResult.partnerOverlappedLanes.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedLanes.get(partnerId).size()) : "0";
                data[row++] = sameSideOverlapResult.partnerOverlappedMyLoads.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedMyLoads.get(partnerId).size()) : "0";
                data[row++] = sameSideOverlapResult.partnerOverlappedLoads.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedLoads.get(partnerId).size()) : "0";
                data[row++] = sameSideOverlapResult.partnerOverlappedMyCarriers.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedMyCarriers.get(partnerId).size()) : "0";
                data[row++] = sameSideOverlapResult.partnerOverlappedCarriers.containsKey(partnerId) ? String.valueOf(sameSideOverlapResult.partnerOverlappedCarriers.get(partnerId).size()) : "0";

                data[row++] = circularOverlapResult.partnerOverlappedMyLanes.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedMyLanes.get(partnerId).size()) : "0";
                data[row++] = circularOverlapResult.partnerOverlappedLanes.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedLanes.get(partnerId).size()) : "0";
                data[row++] = circularOverlapResult.partnerOverlappedMyLoads.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedMyLoads.get(partnerId).size()) : "0";
                data[row++] = circularOverlapResult.partnerOverlappedLoads.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedLoads.get(partnerId).size()) : "0";
                data[row++] = circularOverlapResult.partnerOverlappedMyCarriers.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedMyCarriers.get(partnerId).size()) : "0";
                data[row++] = circularOverlapResult.partnerOverlappedCarriers.containsKey(partnerId) ? String.valueOf(circularOverlapResult.partnerOverlappedCarriers.get(partnerId).size()) : "0";

                data[row++] = continuousRunOverlapResult.partnerOverlappedMyLanes.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedMyLanes.get(partnerId).size()) : "0";
                data[row++] = continuousRunOverlapResult.partnerOverlappedLanes.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedLanes.get(partnerId).size()) : "0";
                data[row++] = continuousRunOverlapResult.partnerOverlappedMyLoads.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedMyLoads.get(partnerId).size()) : "0";
                data[row++] = continuousRunOverlapResult.partnerOverlappedLoads.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedLoads.get(partnerId).size()) : "0";
                data[row++] = continuousRunOverlapResult.partnerOverlappedMyCarriers.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedMyCarriers.get(partnerId).size()) : "0";
                data[row++] = continuousRunOverlapResult.partnerOverlappedCarriers.containsKey(partnerId) ? String.valueOf(continuousRunOverlapResult.partnerOverlappedCarriers.get(partnerId).size()) : "0";

                writer.writeNext(data);
            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
        }
    }

    private void importPartnerMaskedMap(){
        try (
                Reader reader = Files.newBufferedReader(java.nio.file.Paths.get(partnerMaskedMapLoadFile));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {
            // Reading Records One by One in a String array
            String[] nextRecord;
            while ((nextRecord = csvReader.readNext()) != null) {
                partnerMaskedMap.put(nextRecord[0],nextRecord[1]);
                partnerCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void importCarrierMaskedMap(){
        try (
                Reader reader = Files.newBufferedReader(java.nio.file.Paths.get(carrierMaskedMapLoadFile));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {
            // Reading Records One by One in a String array
            String[] nextRecord;
            while ((nextRecord = csvReader.readNext()) != null) {
                carrierMaskedMap.put(nextRecord[0],nextRecord[1]);
                int carrierCount = 0;
                if(partnerCarrierCount.containsKey(nextRecord[0])){
                    carrierCount = partnerCarrierCount.get(nextRecord[0]);
                }
                carrierCount++;
                partnerCarrierCount.put(nextRecord[0], carrierCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printLanesJson() throws Exception {
        JSONObject lanesObject = new JSONObject();
        JSONArray lanes = new JSONArray();
        for (LongObjectCursor<Node> laneCursor : laneMap) {
            long laneId = laneCursor.key;
            Node lane = laneCursor.value;
            JSONObject laneObject = new JSONObject();
            String start_city = (String) lane.getProperty("start_city");
            String start_state = (String) lane.getProperty("start_state");
            String end_city = (String) lane.getProperty("end_city");
            String end_state = (String) lane.getProperty("end_state");
            originCities.add(start_city+","+start_state);
            destinationCities.add(end_city+","+end_state);
            laneObject.put("laneId", laneId);
            laneObject.put("start_city_name", start_city);
            laneObject.put("start_city_state", start_state);
            laneObject.put("end_city_name", end_city);
            laneObject.put("end_city_state", end_state);
            laneObject.put("start_city_latitude", lane.getProperty("start_city_latitude"));
            laneObject.put("start_city_longitude", lane.getProperty("start_city_longitude"));
            laneObject.put("end_city_latitude", lane.getProperty("end_city_latitude"));
            laneObject.put("end_city_longitude", lane.getProperty("end_city_longitude"));
            laneObject.put("load_count", laneLoads.get(laneId).size());
            laneObject.put("carrier_count", laneCarriers.get(laneId).size());
            lanes.add(laneObject);
        }
        lanesObject.put("shipper_lanes", lanes);
        Files.write(java.nio.file.Paths.get(filename+"_shipper_lanes.json"), lanesObject.toJSONString().getBytes());
    }

    private void printPartnerJsonFiles(OverlapResult overlapResult, String tag) throws Exception{
        for(Map.Entry<String, JSONArray> partnerLanes : overlapResult.partnerLanesJson.entrySet()){
            String partnerId = partnerLanes.getKey();
            String maskerPartnerId = getMaskedParnter(partnerId);
            JSONArray lanesJson = partnerLanes.getValue();
            JSONObject partnerJson = new JSONObject();
            partnerJson.put(maskerPartnerId, lanesJson);
            Files.write(java.nio.file.Paths.get(filename+"_"+maskerPartnerId+"_"+tag+".json"), partnerJson.toJSONString().getBytes());
        }
    }

    private void printPartnerLanes() throws Exception{
        for(ObjectCursor<String> partnerCursor : partnersList){
            String partnerId = partnerCursor.value;
            String maskedPartnerId = getMaskedParnter(partnerId);
            JSONObject lanesObject = new JSONObject();
            JSONArray lanes = new JSONArray();
            LongHashSet allOverlaps = new LongHashSet();
            allOverlaps.addAll(sameSideOverlapResult.partnerOverlappedLanes.getOrDefault(partnerId, new LongHashSet()));
            allOverlaps.addAll(circularOverlapResult.partnerOverlappedLanes.getOrDefault(partnerId, new LongHashSet()));
            allOverlaps.addAll(continuousRunOverlapResult.partnerOverlappedLanes.getOrDefault(partnerId, new LongHashSet()));
            for(LongCursor allOverlapsCursor: allOverlaps){
                Long overlapLaneId = allOverlapsCursor.value;
                Node overlapLane = dbService.getNodeById(overlapLaneId);
                JSONObject laneObject = new JSONObject();
                String query = "MATCH (lane:Lane) where id(lane) = {laneId} " +
                        "MATCH (lane)<-[LOAD_LANE]-(load:Load) WHERE load.shipper_id = {partnerId} " +
                        "RETURN count(distinct load) as load_count, count(distinct load.carrier_id) as carrier_count";
                Map<String, Object> params = new HashMap<>();
                params.put("laneId", overlapLaneId);
                params.put("partnerId", partnerId);
                Result queryResult = dbService.execute(query, params);
                laneObject.put("laneId", overlapLaneId);
                String start_city = (String) overlapLane.getProperty("start_city");
                String start_state = (String) overlapLane.getProperty("start_state");
                String end_city = (String) overlapLane.getProperty("end_city");
                String end_state = (String) overlapLane.getProperty("end_state");
                originCities.add(start_city+","+start_state);
                destinationCities.add(end_city+","+end_state);
                laneObject.put("start_city_name", start_city);
                laneObject.put("start_city_state", start_state);
                laneObject.put("end_city_name", end_city);
                laneObject.put("end_city_state", end_state);
                laneObject.put("start_city_latitude", overlapLane.getProperty("start_city_latitude"));
                laneObject.put("start_city_longitude", overlapLane.getProperty("start_city_longitude"));
                laneObject.put("end_city_latitude", overlapLane.getProperty("end_city_latitude"));
                laneObject.put("end_city_longitude", overlapLane.getProperty("end_city_longitude"));
                if(queryResult.hasNext()) {
                    Map<String, Object> value = queryResult.next();
                    laneObject.put("load_count", value.get("load_count"));
                    laneObject.put("carrier_count", value.get("carrier_count"));
                }
                lanes.add(laneObject);
            }
            lanesObject.put(maskedPartnerId, lanes);
            Files.write(java.nio.file.Paths.get(filename+"_"+maskedPartnerId+"_lanes.json"), lanesObject.toJSONString().getBytes());
        }
    }

    public void printPartnerFiles() throws Exception{
        JSONObject partnersObject = new JSONObject();
        JSONArray partnersArray = new JSONArray();
        for(ObjectCursor<String> partnerCursor : partnersList){
            JSONObject partnerFiles = new JSONObject();
            String partnerId = getMaskedParnter(partnerCursor.value);
            partnerFiles.put("partnerId", partnerId);
            partnerFiles.put("lanes_file",companyId+"_"+partnerId+"_"+"lanes.json");
            partnerFiles.put("same_side_overlaps_file",companyId+"_"+partnerId+"_"+"same_side_overlaps.json");
            partnerFiles.put("round_trip_overlaps_file",companyId+"_"+partnerId+"_"+"round_trip_overlaps.json");
            partnerFiles.put("continuous_run_overlaps_file",companyId+"_"+partnerId+"_"+"continuous_run_overlaps.json");
            partnersArray.add(partnerFiles);
        }
        partnersObject.put("partners", partnersArray);
        Files.write(java.nio.file.Paths.get(filename+"_"+"partner_files.json"), partnersObject.toJSONString().getBytes());
    }

    public void printShipperFile() throws Exception{
        JSONObject shipperObject = new JSONObject();
        shipperObject.put("shipper", companyId);
        Files.write(java.nio.file.Paths.get(directory+"shipper_name.json"), shipperObject.toJSONString().getBytes());
    }


    public void printStringSet(String tag, Set<String> stringSet) throws Exception{
        JSONObject object = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        for(String tempString : stringSet){
            jsonArray.add(tempString);
        }
        object.put(tag, jsonArray);
        Files.write(java.nio.file.Paths.get(filename+"_"+tag+".json"), object.toJSONString().getBytes());
    }

    public AnalysisAlgo compute() throws Exception{
        initialize();
        loadLanes();
        printShipperFile();
        printLanesJson();
        if(partnerMaskedMapLoadFile != null){
            importPartnerMaskedMap();
        }

        if(carrierMaskedMapLoadFile != null){
            importCarrierMaskedMap();
        }
        calculateLaneOverlaps();

        String[] laneOverlapHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "overlap_lane_start_city", "overlap_lane_start_state", "overlap_lane_end_city", "overlap_lane_end_state", "partner"};
        String[] withoutCarriersHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper", "my_loads", "my_carriers","partner", "No. of lane overlaps", "partner_loads", "partner_carriers"};
        String[] withCarriersHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper" ,"my_loads", "my_carriers", "partner", "partner_carrier", "overlaps", "partner_carrier_loads"};

        printResultsToFile(filename+"_same_side_lane_overlaps.csv", sameSideOverlap, sameSideOverlapResult, true,false, laneOverlapHeader);
        printResultsToFile(filename+"_same_side_overlap_without_carriers.csv", sameSideOverlap, sameSideOverlapResult, false,true, withoutCarriersHeader);
        printResultsToFile(filename+"_same_side_overlap_with_carriers.csv", sameSideOverlap, sameSideOverlapResult, false,false, withCarriersHeader);

        printPartnerJsonFiles(sameSideOverlapResult, "same_side_overlaps");

        printResultsToFile(filename+"_round_trip_overlaps.csv", circularOverlap, circularOverlapResult, true,false, laneOverlapHeader);
        printResultsToFile(filename+"_round_trip_without_carriers.csv", circularOverlap, circularOverlapResult, false,true, withoutCarriersHeader);
        printResultsToFile(filename+"_round_trip_with_carriers.csv", circularOverlap, circularOverlapResult, false,false, withCarriersHeader);
        printPartnerJsonFiles(circularOverlapResult, "round_trip_overlaps");

        printResultsToFile(filename+"_continuous_run_lane_overlaps.csv", continuousRunOverlap, continuousRunOverlapResult, true,false, laneOverlapHeader);
        printResultsToFile(filename+"_continuous_run_overlap_without_carriers.csv", continuousRunOverlap, continuousRunOverlapResult, false, true, withoutCarriersHeader);
        printResultsToFile(filename+"_continuous_run_with_carriers.csv", continuousRunOverlap, continuousRunOverlapResult,false,false, withCarriersHeader);
        printPartnerJsonFiles(continuousRunOverlapResult, "continuous_run_overlaps");

        printPartnerLanes();
        printPartnerFiles();
        printStringSet("origin_cities", originCities);
        printStringSet("destination_cities", destinationCities);

        printMaskedPartnerFile();
        printMaskedCarrierFile();

        printSummaryFile();
        printLaneSummaryFile();
        return this;
    }

    private static double bearing(double lat1, double lon1, double lat2, double lon2){
        double latitude1 = Math.toRadians(lat1);
        double latitude2 = Math.toRadians(lat2);
        double longDiff= Math.toRadians(lon2 - lon1);
        double y= Math.sin(longDiff)*Math.cos(latitude2);
        double x=Math.cos(latitude1)*Math.sin(latitude2)-Math.sin(latitude1)*Math.cos(latitude2)*Math.cos(longDiff);
        return (Math.toDegrees(Math.atan2(y, x))+360)%360;
    }

    @Override
    public AnalysisAlgo me() {
        return this;
    }

    @Override
    public AnalysisAlgo release() {
        return this;
    }
}
