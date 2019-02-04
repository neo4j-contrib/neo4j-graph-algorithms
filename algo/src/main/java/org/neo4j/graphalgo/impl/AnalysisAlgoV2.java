package org.neo4j.graphalgo.impl;

import au.com.bytecode.opencsv.CSVWriter;
import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.results.OverlapResultV2;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.mapdb.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;

public class AnalysisAlgoV2 extends Algorithm<AnalysisAlgoV2> {
    private final GraphDatabaseAPI dbService;
    private final Log log;

    private String companyId;
    private String filename;

    private String loadLaneRelName;
    private String laneStartName;
    private String laneEndName;

    private String fileOrMemory;
    private int initialBlockSize;
    private int incrementalBlockSize;

    private double deadHeadPercentage;
    private double offRoutePercentage;

    private int travelDirectionDeviation;

    private LongLongMap loadLaneMap;
    private LongObjectMap<Node> loadMap;

    private LongObjectMap<LongScatterSet> laneLoads;
    private LongObjectMap<ObjectHashSet<String>> laneCarriers;
    private LongObjectMap<Node> laneMap;

    private HTreeMap<Long, long[]> sameSideOverlap;
    private HTreeMap<Long, long[]> circularOverlap;
    private HTreeMap<Long, long[]> continuousRunOverlap;

    private OverlapResultV2 sameSideOverlapResult;
    private OverlapResultV2 circularOverlapResult;
    private OverlapResultV2 continuousRunOverlapResult;

    private Set<String> partnersList;

//    private LongObjectMap<Node> overlapLaneMap;

    private Map<String, String> partnerMaskedMap;

    private Map<String, Integer> partnerCarrierCount;

    private Map<String, String> carrierMaskedMap;

    private String partnerMaskedMapLoadFile;
    private String carrierMaskedMapLoadFile;

    private DB db;

    private int partnerCount;

    public AnalysisAlgoV2(final String companyId, final String filename, final ProcedureConfiguration configuration, final GraphDatabaseAPI dbService, final Log log) {
        this.companyId = companyId;
        this.filename = filename;
        this.dbService = dbService;
        this.log = log;
        this.loadLaneRelName = configuration.get("loadLaneRelName", "LOAD_LANE");
        this.laneStartName = configuration.get("laneStartName", "LANE_START_CITY");
        this.laneEndName = configuration.get("laneEndName", "LANE_END_CITY");
        this.offRoutePercentage = configuration.get("offRoutePercentage", 1.50);
        this.deadHeadPercentage = configuration.get("deadHeadPercentage", 0.50);
        this.travelDirectionDeviation = configuration.get("travelDirectionDeviation", 60);
        this.fileOrMemory = configuration.get("fileOrMemory", "file");
        this.initialBlockSize = configuration.get("initialBlockSize", 5);
        this.incrementalBlockSize = configuration.get("incrementalBlockSize", 512);
        this.partnerMaskedMapLoadFile = configuration.get("partnerMaskedMapLoadFile", null);
        this.carrierMaskedMapLoadFile = configuration.get("carrierMaskedMapLoadFile", null);
    }

    public void initialize(){
        if(fileOrMemory.equalsIgnoreCase("file")) {
            db = DBMaker
                    .fileDB(filename + ".db")
                    .fileChannelEnable()
                    .fileMmapEnable()
                    .concurrencyDisable()
                    .fileLockDisable()
                    .checksumHeaderBypass()
                    .allocateStartSize(initialBlockSize * 1024 * 1024 * 1024)
                    .allocateIncrement(incrementalBlockSize * 1024 * 1024)
                    .make();
        }
        else{
            db = DBMaker
                    .memoryDB()
                    .allocateStartSize(initialBlockSize * 1024 * 1024 * 1024)
                    .allocateIncrement(incrementalBlockSize * 1024 * 1024)
                    .make();
        }
        loadMap = new LongObjectHashMap<>();
        loadLaneMap = new LongLongHashMap();

        laneLoads = new LongObjectScatterMap<>();
        laneCarriers = new LongObjectHashMap<>();
        laneMap = new LongObjectScatterMap<>();

        sameSideOverlap = db.hashMap("sameSideOverlap")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG_ARRAY)
                .createOrOpen();
        circularOverlap = db.hashMap("circularOverlap")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG_ARRAY)
                .createOrOpen();
        continuousRunOverlap = db.hashMap("continuousRunOverlap")
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG_ARRAY)
                .createOrOpen();

//        overlapLaneMap = new LongObjectScatterMap<>();

        partnerMaskedMap = db
                .hashMap("partnerMaskedMap", Serializer.STRING, Serializer.STRING)
                .createOrOpen();

        partnerCarrierCount = db
                .hashMap("partnerCarrierCount", Serializer.STRING, Serializer.INTEGER)
                .createOrOpen();
        carrierMaskedMap = db
                .hashMap("carrierMaskedMap", Serializer.STRING, Serializer.STRING)
                .createOrOpen();

        partnerCount = partnerMaskedMap.size();

        sameSideOverlapResult = new OverlapResultV2(db, "sameSide");
        circularOverlapResult = new OverlapResultV2(db, "circular");
        continuousRunOverlapResult = new OverlapResultV2(db, "continuousRun");

        partnersList = db
                .treeSet("partnersList", Serializer.STRING)
                .createOrOpen();
    }


    private void loadLanes(){
        long startTime;
        long endTime;
        startTime = System.nanoTime();
        ResourceIterator<Node> loads = dbService.findNodes(Label.label("Load"), "shipper_id", companyId);
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
            index += 1;
            TraversalDescription routeTd = dbService.traversalDescription()
                    .breadthFirst()
                    .relationships(RelationshipType.withName("ROUTE"), Direction.BOTH)
                    .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                    .evaluator(Evaluators.atDepth(1));
            Long laneId = laneLoad.key;
            Node lane = laneMap.get(laneId);
            Set<Long> tempSameSideOverlap = new HashSet<>();
            Set<Long> tempCircularOverlap = new HashSet<>();
            Set<Long> tempContinuousRunOverlap = new HashSet<>();
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
                    //overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
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
                    if(secondArchEnd != null){
                        if( (overlapTravelDirection > secondArchStart && overlapTravelDirection < secondArchEnd) || (overlapTravelDirection > firstArchStart && overlapTravelDirection < firstArchEnd) ) {
                                tempSameSideOverlap.add(possibleOverlapLane.getId());
                        }
                    }
                    else {
                        if ( overlapTravelDirection > firstArchStart && overlapTravelDirection < firstArchEnd ) {
                            tempSameSideOverlap.add(possibleOverlapLane.getId());
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
                    //overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
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
                        if( (overlapTravelDirection > rsecondArchStart && overlapTravelDirection < rsecondArchEnd) || (overlapTravelDirection > rfirstArchStart && overlapTravelDirection < rfirstArchEnd)) {
                            tempCircularOverlap.add(possibleOverlapLane.getId());
                        } else {
                            tempContinuousRunOverlap.add(possibleOverlapLane.getId());
                        }
                    }
                    else {
                        if ( overlapTravelDirection > rfirstArchStart && overlapTravelDirection < rfirstArchEnd ) {
                            tempCircularOverlap.add(possibleOverlapLane.getId());
                        } else{
                            tempContinuousRunOverlap.add(possibleOverlapLane.getId());
                        }
                    }

                }
                sameSideOverlap.putIfAbsent(laneId, tempSameSideOverlap.stream().mapToLong(l -> l).toArray());
                circularOverlap.putIfAbsent(laneId, tempCircularOverlap.stream().mapToLong(l -> l).toArray());
                continuousRunOverlap.putIfAbsent(laneId, tempContinuousRunOverlap.stream().mapToLong(l -> l).toArray());
            }
            endTime = System.nanoTime();
            log.info("Completed lane Calculation: " + index + " out of " + laneLoads.size() + " in " + (endTime - startTime) + " ns");
        }
    }

    private String getMaskedParnter(String partner){
        if(partnerMaskedMap.containsKey(partner)){
            return partnerMaskedMap.get(partner);
        }

        partnerCount++;
        String maskedPartner = String.format("partner%s", Integer.toString(partnerCount));
        partnerMaskedMap.put(partner, maskedPartner);

        return maskedPartner;
    }

    private String getMaskedCarrier(String partner, String carrier){
        String partnerCarrier = String.format("%s|%s", partner, carrier);
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
        carrierMaskedMap.put(partnerCarrier, maskedCarrier);

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
            for (Map.Entry<String, String> entry : partnerMaskedMap.entrySet() ){
                String[] data = new String[2];
                int row = 0;
                data[row++] = entry.getKey();
                data[row++] = entry.getValue();
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
            for (Map.Entry<String, String> entry : carrierMaskedMap.entrySet()) {
                String[] data = new String[2];
                int row = 0;
                data[row++] = entry.getKey();
                data[row++] = entry.getValue();
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
                data[row++] = sameSideOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.lanePartners.get(laneId)) : "0";
                data[row++] = sameSideOverlapResult.lanePartnerOverlaps.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.lanePartnerOverlaps.get(laneId)) : "0" ;
                data[row++] = sameSideOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.lanePartnerLoads.get(laneId)) : "0" ;
                data[row++] = sameSideOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(sameSideOverlapResult.laneCarriers.get(laneId)) : "0" ;
                data[row++] = circularOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(circularOverlapResult.lanePartners.get(laneId)) : "0" ;
                data[row++] = circularOverlapResult.lanePartnerOverlaps.containsKey(laneId) ? String.valueOf(circularOverlapResult.lanePartnerOverlaps.get(laneId)) : "0" ;
                data[row++] = circularOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(circularOverlapResult.lanePartnerLoads.get(laneId)) : "0" ;
                data[row++] = circularOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(circularOverlapResult.laneCarriers.get(laneId)) : "0" ;
                data[row++] = continuousRunOverlapResult.lanePartners.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.lanePartners.get(laneId)) : "0" ;
                data[row++] = continuousRunOverlapResult.lanePartnerOverlaps.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.lanePartnerOverlaps.get(laneId)) : "0" ;
                data[row++] = continuousRunOverlapResult.lanePartnerLoads.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.lanePartnerLoads.get(laneId)) : "0" ;
                data[row++] = continuousRunOverlapResult.laneCarriers.containsKey(laneId) ? String.valueOf(continuousRunOverlapResult.laneCarriers.get(laneId)) : "0" ;

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

    private void printResultsToFile(String filename, HTreeMap<Long, long[]> Overlap, OverlapResultV2 overlapResult , boolean isLaneOverlapReport, boolean isSummary, String[] header){
        //results Printer
        File laneSummaryfile = new File(filename);
        try {
            FileWriter fileOutput = new FileWriter(laneSummaryfile);
            CSVWriter writer = new CSVWriter(fileOutput);
            writer.writeNext(header);
            int index = 0;
            for (LongObjectCursor<Node> laneCursor : laneMap) {
                index++;
                long startTime =0;
                long endTime = 0;
                Node lane = laneCursor.value;
                long laneId = lane.getId();
                String[] data = new String[header.length];
                int row = 0;
                data[row++] = (String) lane.getProperty("start_city");
                data[row++] = (String) lane.getProperty("start_state");
                data[row++] = (String) lane.getProperty("end_city");
                data[row++] = (String) lane.getProperty("end_state");
                if(Overlap.containsKey(laneId)){
                    startTime = System.nanoTime();
                    long[] overlaps = Overlap.get(laneId);
                    ObjectObjectMap<String, LongHashSet> partnerLoadMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> partnerOverlapMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, ObjectHashSet<String>> partnerCarrierMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> carrierLoadMap = new ObjectObjectHashMap<>();
                    ObjectObjectMap<String, LongHashSet> carrierLaneMap = new ObjectObjectHashMap<>();
                    for (Long overlapLaneId : overlaps) {
                        Node overlapLane = dbService.getNodeById(overlapLaneId);
                        if (!overlapLane.hasRelationship(RelationshipType.withName(loadLaneRelName), Direction.INCOMING)) {
//                            overlaps.remove(overlapLaneId);
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

                            partnersList.add(partnerId);

                        }
                    }

                    Set<Long> laneSummaryPartnerLoadIds = new HashSet<>();
                    Set<String> lanePartnerIds = new HashSet<>();

                    for(ObjectObjectCursor<String, LongHashSet> partnerCursor: partnerLoadMap){
                        String partnerId = partnerCursor.key;

                        Set<Long> partnerOverlappedLoadIds = overlapResult.partnerOverlappedLoads.computeIfAbsent(partnerId, k -> new HashSet<>());
                        for(LongCursor tempLoadId : partnerCursor.value){
                            partnerOverlappedLoadIds.add(tempLoadId.value);
                            laneSummaryPartnerLoadIds.add(tempLoadId.value);
                        }
//                        overlapResult.partnerOverlappedLoads.put(partnerId, partnerOverlappedLoadIds);

                        Set<Long> shipperOverlappedLaneIds = overlapResult.partnerOverlappedMyLanes.computeIfAbsent(partnerId, k -> new HashSet<>());
                        shipperOverlappedLaneIds.add(laneId);
//                        overlapResult.partnerOverlappedMyLanes.put(partnerId, shipperOverlappedLaneIds);

                        Set<Long> shipperOverlappedLoadIds = overlapResult.partnerOverlappedMyLoads.computeIfAbsent(partnerId, k -> new HashSet<>());
                        for(LongCursor tempLoadId : laneLoads.get(laneId)){
                            shipperOverlappedLoadIds.add(tempLoadId.value);
                        }
//                        overlapResult.partnerOverlappedMyLoads.put(partnerId, shipperOverlappedLoadIds);

                        Set<String> shipperOverlappedCarrierIds = overlapResult.partnerOverlappedMyCarriers.computeIfAbsent(partnerId, k -> new HashSet<>());
                        for(ObjectCursor<String> tempCarrierId : laneCarriers.get(laneId)){
                            shipperOverlappedCarrierIds.add(tempCarrierId.value);
                        }
//                        overlapResult.partnerOverlappedMyCarriers.put(partnerId, shipperOverlappedCarrierIds);


                        lanePartnerIds.add(partnerId);
                    }
                    overlapResult.lanePartnerLoads.put(laneId, laneSummaryPartnerLoadIds.size());
                    overlapResult.lanePartners.put(laneId, lanePartnerIds.size());

                    Set<Long> lanePartnerOverlapsIds = new HashSet<>();

                    for(ObjectObjectCursor<String, LongHashSet> partnerOverlapCursor: partnerOverlapMap){
                        String partnerId = partnerOverlapCursor.key;
                        Set<Long> partnerOverlappedLaneIds = overlapResult.partnerOverlappedLanes.computeIfAbsent(partnerId, k -> new HashSet<>());
                        for(LongCursor tempOverlapLaneId : partnerOverlapCursor.value){
                            partnerOverlappedLaneIds.add(tempOverlapLaneId.value);
                            lanePartnerOverlapsIds.add(tempOverlapLaneId.value);
                        }
//                        overlapResult.partnerOverlappedLanes.put(partnerId, partnerOverlappedLaneIds);
                    }
                    overlapResult.lanePartnerOverlaps.put(laneId, lanePartnerOverlapsIds.size());

                    Set<String> laneCarriersIds = new HashSet<>();
                    for(ObjectObjectCursor<String, ObjectHashSet<String>> partnerCarrierCursor: partnerCarrierMap){
                        String partnerId = partnerCarrierCursor.key;
                        Set<String> partnerOverlappedCarrierIds = overlapResult.partnerOverlappedCarriers.computeIfAbsent(partnerId, k -> new HashSet<>());
                        for(ObjectCursor<String> tempCarrierId : partnerCarrierCursor.value){
                            partnerOverlappedCarrierIds.add(tempCarrierId.value);
                            laneCarriersIds.add(tempCarrierId.value);
                        }
//                        overlapResult.partnerOverlappedCarriers.put(partnerId, partnerOverlappedCarrierIds);
                    }
                    overlapResult.laneCarriers.put(laneId, laneCarriersIds.size());



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
                }
                endTime = System.nanoTime();
                log.info("Completed lane Calculation: " + index + " out of " + laneLoads.size() + " in " + (endTime - startTime) + " ns");
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


    private void printOverlapFile(String filename, HTreeMap<Long, Set<Long>> Overlap){
        File summaryFile = new File(filename);
        try {
            FileWriter fileOutput = new FileWriter(summaryFile);
            CSVWriter writer = new CSVWriter(fileOutput);
            String[] header = {"lane_id","overlap_id"};
            writer.writeNext(header);
            for ( Map.Entry<Long, Set<Long> > cursor : Overlap.entrySet() ) {
                String[] data = new String[header.length];
                int row = 0;
                data[row++] = Long.toString(cursor.getKey());
                Set<Long> overlaps = cursor.getValue();
                for (Long overlap : overlaps) {
                    String[] writeData = data.clone();
                    int innerRow = row;
                    writeData[innerRow++] = Long.toString(overlap);
                    writer.writeNext(writeData);
                }
            }
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            log.info(e.getMessage());
            e.printStackTrace();
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
            Iterator<String> partnersIterator = partnersList.iterator();
            while(partnersIterator.hasNext())
            {
                String partnerId = partnersIterator.next();
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

    public AnalysisAlgoV2 compute(){
        initialize();
        loadLanes();
//        printOverlapFile("same_side_overlaps.csv", sameSideOverlap);
//        printOverlapFile("circular_overlaps.csv", circularOverlap);
//        printOverlapFile("continuous_run_overlaps.csv", continuousRunOverlap);
//        calculateLaneOverlaps();

        if(partnerMaskedMapLoadFile != null){
            importPartnerMaskedMap();
        }

        if(carrierMaskedMapLoadFile != null){
            importCarrierMaskedMap();
        }

        String[] laneOverlapHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "overlap_lane_start_city", "overlap_lane_start_state", "overlap_lane_end_city", "overlap_lane_end_state", "partner"};
        String[] withoutCarriersHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper", "my_loads", "my_carriers","partner", "No. of lane overlaps", "partner_loads", "partner_carriers"};
        String[] withCarriersHeader = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper" ,"my_loads", "my_carriers", "partner", "partner_carrier", "overlaps", "partner_carrier_loads"};


//        printResultsToFile(filename+"_same_side_lane_overlaps.csv", sameSideOverlap, sameSideOverlapResult, true,false, laneOverlapHeader);
//        printResultsToFile(filename+"_same_side_overlap_without_carriers.csv", sameSideOverlap, sameSideOverlapResult, false,true, withoutCarriersHeader);
        printResultsToFile(filename+"_same_side_overlap_with_carriers.csv", sameSideOverlap, sameSideOverlapResult, false,false, withCarriersHeader);
//
//        printResultsToFile(filename+"_circular_lane_overlaps.csv", circularOverlap, circularOverlapResult, true,false, laneOverlapHeader);
//        printResultsToFile(filename+"_circular_overlap_without_carriers.csv", circularOverlap, circularOverlapResult, false,true, withoutCarriersHeader);
        printResultsToFile(filename+"_circular_overlap_with_carriers.csv", circularOverlap, circularOverlapResult, false,false, withCarriersHeader);
//
//        printResultsToFile(filename+"_continuous_run_lane_overlaps.csv", continuousRunOverlap, continuousRunOverlapResult, true,false, laneOverlapHeader);
//        printResultsToFile(filename+"_continuous_run_overlap_without_carriers.csv", continuousRunOverlap, continuousRunOverlapResult, false, true, withoutCarriersHeader);
        printResultsToFile(filename+"_continuous_run_with_carriers.csv", continuousRunOverlap, continuousRunOverlapResult,false,false, withCarriersHeader);
//
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
    public AnalysisAlgoV2 me() {
        return this;
    }

    @Override
    public AnalysisAlgoV2 release() {
        return this;
    }
}
