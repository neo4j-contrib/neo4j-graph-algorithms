package org.neo4j.graphalgo.impl;

import au.com.bytecode.opencsv.CSVWriter;
import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class AnalysisAlgo extends Algorithm<AnalysisAlgo> {
    private final GraphDatabaseAPI dbService;
    private static final int PATH_END = -1;
    private final Log log;

    private String companyId;
    private String filename;

    private String loadLaneRelName;
    private String laneStartName;
    private String laneEndName;

    private ResourceIterator<Node> loads;

    private LongLongMap loadLaneMap;
    private LongObjectMap<Node> loadMap;

    private LongObjectMap<LongScatterSet> laneLoads;
    private LongObjectMap<Set<String>> laneCarriers;
    private LongObjectMap<Node> laneMap;

    private LongObjectMap<LongScatterSet> sameSideOverlap;
    private LongObjectMap<LongScatterSet> circularOverlap;
    private LongObjectMap<LongScatterSet> continuousRunOverlap;

    private LongObjectMap<Node> overlapLaneMap;

    public AnalysisAlgo(final String companyId, final String filename, final ProcedureConfiguration configuration, final GraphDatabaseAPI dbService, final Log log) {
        this.companyId = companyId;
        this.filename = filename;
        this.dbService = dbService;
        this.log = log;
        this.loadLaneRelName = configuration.get("loadLaneRelName", "LOAD_LANE");
        this.laneStartName = configuration.get("laneStartName", "LANE_START_CITY");
        this.laneEndName = configuration.get("laneEndName", "LANE_END_CITY");
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

        overlapLaneMap = new LongObjectScatterMap<>();
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
            Set<String> carriers = laneCarriers.get(lane.getId());
            if( carriers == null ){
                carriers = new HashSet<>();
                laneCarriers.put(lane.getId(), carriers);
            }
            carriers.add((String) load.getProperty("carrier_id"));
        });
        endTime = System.nanoTime();
        log.info("Completed Load Get and Lane Create " + (endTime - startTime) + " Size " + loadMap.size());
    }

    private void calculateLaneOverlaps(){
        long startTime;
        long endTime;
        TraversalDescription routeTd = dbService.traversalDescription()
                .breadthFirst()
                .relationships(RelationshipType.withName("ROUTE"), Direction.BOTH)
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .evaluator(Evaluators.atDepth(1));
        int index = 0;
        for (LongObjectCursor<LongScatterSet> laneLoad : laneLoads) {
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
            Traverser lpcRouteTraverse = routeTd.traverse(lpc);
            for (Path routePath : lpcRouteTraverse) {
                Relationship cocRel = routePath.lastRelationship();
                double pathDistance = (double) cocRel.getProperty("distance");
                if (pathDistance > laneDistance * .50) {
                    continue;
                }
                Node coc = cocRel.getOtherNode(lpc);
                Iterable<Relationship> cocLaneRels = coc.getRelationships(RelationshipType.withName(laneStartName), Direction.INCOMING);
                for (Relationship cocLaneRel : cocLaneRels) {
                    Node possibleOverlapLane = cocLaneRel.getStartNode();
                    if (pathDistance + (double) possibleOverlapLane.getProperty("distance") > laneDistance * 1.5) {
                        continue;
                    }
                    overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
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
                    if (overlapTravelDirection > travelDirection - 60 && overlapTravelDirection < travelDirection + 60) {
                        LongScatterSet sameSideOverlaps = sameSideOverlap.get(lane.getId());
                        if (null == sameSideOverlaps) {
                            sameSideOverlaps = new LongScatterSet();
                            sameSideOverlap.put(lane.getId(), sameSideOverlaps);
                        }
                        sameSideOverlaps.add(possibleOverlapLane.getId());
                    } else if (overlapTravelDirection > reverseTravelDirection - 60 && overlapTravelDirection < reverseTravelDirection + 60) {
                        LongScatterSet circularOverlaps = circularOverlap.get(lane.getId());
                        if (null == circularOverlaps) {
                            circularOverlaps = new LongScatterSet();
                            circularOverlap.put(lane.getId(), circularOverlaps);
                        }
                        circularOverlaps.add(possibleOverlapLane.getId());
                    } else if (overlapTravelDirection > reverseTravelDirection + 60 && overlapTravelDirection < reverseTravelDirection - 60) {
                        LongScatterSet continuousRunOverlaps = circularOverlap.get(lane.getId());
                        if (null == continuousRunOverlaps) {
                            continuousRunOverlaps = new LongScatterSet();
                            continuousRunOverlap.put(lane.getId(), continuousRunOverlaps);
                        }
                        continuousRunOverlaps.add(possibleOverlapLane.getId());
                    }
                }
            }
            endTime = System.nanoTime();
            log.info("Completed lane Calculation: " + index + " out of " + laneLoads.size() + " in " + (endTime - startTime) + " ns");
        }
    }

    private void printResultsToFile(String filename, LongObjectMap<LongScatterSet> Overlap, boolean isSummary, String[] header){
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
                        Node overlapLane = overlapLaneMap.get(overlapLaneId);
                        if (!overlapLane.hasRelationship(RelationshipType.withName(loadLaneRelName), Direction.INCOMING)) {
                            overlaps.remove(overlapLaneId);
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
                        writeData[innerRow++] = partner;
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
                                    completeData[completeRow++] = carrierName;
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

    public AnalysisAlgo compute(){
        initialize();
        loadLanes();
        calculateLaneOverlaps();
        String[] header = {"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper", "my_loads", "my_carriers","partner", "No. of lane overlaps", "partner_loads", "partner_carriers"};
        printResultsToFile(filename+"_same_side_overlap_summary.csv", sameSideOverlap,true, header);
        printResultsToFile(filename+"_circular_overlap_summary.csv", circularOverlap, true, header);
        printResultsToFile(filename+"_continuous_run_overlap_summary.csv", continuousRunOverlap, true, header);

        header = new String[]{"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "shipper" ,"my_loads", "my_carriers", "partner", "partner_carrier", "overlaps", "partner_carrier_loads"};

        printResultsToFile(filename+"_same_side_overlap.csv", sameSideOverlap, false, header);
        printResultsToFile(filename+"_circular_overlap.csv", circularOverlap, false, header);
        printResultsToFile(filename+"_continuous_run_overlap.csv", continuousRunOverlap, false, header);
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
