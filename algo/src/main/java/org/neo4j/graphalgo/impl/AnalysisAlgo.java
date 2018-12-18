package org.neo4j.graphalgo.impl;

import au.com.bytecode.opencsv.CSVWriter;
import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class AnalysisAlgo extends Algorithm<AnalysisAlgo> {
    private final GraphDatabaseAPI dbService;
    private static final int PATH_END = -1;
    private final Log log;

    private String companyId;
    private String filename;
    private String loadPickupRelName;
    private String loadDropRelName;
    private String loadLaneRelName;
    private String laneStartName;
    private String laneEndName;

    public static final double NO_PATH_FOUND = -1.0;

    public AnalysisAlgo(final String companyId, final String filename, final ProcedureConfiguration configuration, final GraphDatabaseAPI dbService, final Log log) {
        this.companyId = companyId;
        this.filename = filename;
        this.dbService = dbService;
        this.log = log;
        this.loadPickupRelName = configuration.get("loadPickupRelName", "LOAD_PICKUP_CITY");
        this.loadDropRelName = configuration.get("loadDropRelName", "LOAD_DROP_CITY");
        this.loadLaneRelName = configuration.get("loadLaneRelName", "LOAD_LANE");
        this.laneStartName = configuration.get("laneStartName", "LANE_START_CITY");
        this.laneEndName = configuration.get("laneEndName", "LANE_END_CITY");
    }

    public AnalysisAlgo compute(){
        ResourceIterator<Node> loads =  dbService.findNodes(Label.label("Load"), "shipper_id", companyId);

        LongLongMap loadLaneMap = new LongLongHashMap();
        LongObjectMap<Node> loadMap = new LongObjectHashMap<>();

        LongObjectMap<LongArrayList> laneLoads = new LongObjectScatterMap<>();
        LongObjectMap<ArrayList<String>> laneCarriers = new LongObjectHashMap<>();
        LongObjectMap<Node> laneMap = new LongObjectScatterMap<>();

        LongObjectMap<LongArrayList> sameSideOverlap = new LongObjectHashMap<>();
        LongObjectMap<LongArrayList> circularOverlap = new LongObjectHashMap<>();
        LongObjectMap<LongArrayList> continuousRunOverlap = new LongObjectHashMap<>();

        LongObjectMap<Node> overlapLaneMap = new LongObjectScatterMap<>();
        long startTime = System.nanoTime();
        long endTime = System.nanoTime();
        startTime = System.nanoTime();
        loads.stream().forEach(load -> {
            loadMap.put(load.getId(), load);
            Node lane = load.getSingleRelationship(RelationshipType.withName(loadLaneRelName), Direction.OUTGOING).getEndNode();
            loadLaneMap.put(load.getId(), lane.getId());
            laneMap.put(lane.getId(), lane);
            LongArrayList loadIds = laneLoads.get(lane.getId());
            if( loadIds == null ){
                loadIds = new LongArrayList();
            }
            loadIds.add(load.getId());
            laneLoads.put(lane.getId(), loadIds);

            ArrayList<String> carriers = laneCarriers.get(lane.getId());
            if( carriers == null ){
                carriers = new ArrayList<>();
            }
            carriers.add((String) load.getProperty("carrier_id"));
            laneCarriers.put(lane.getId(), carriers);
        });
        endTime = System.nanoTime();
        log.debug("Completed Load Get and Lane Create " + (endTime - startTime) + " Size " + loadMap.size());
        TraversalDescription routeTd = dbService.traversalDescription()
                .breadthFirst()
                .relationships(RelationshipType.withName("ROUTE"), Direction.BOTH)
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .evaluator(Evaluators.atDepth(1));
        int index = 0;
        while (laneLoads.iterator().hasNext()){
            index += 1;
            Long laneId = laneLoads.iterator().next().key;
            Node lane = laneMap.get(laneId);
            Node lpc = lane.getSingleRelationship(RelationshipType.withName(laneStartName), Direction.OUTGOING).getEndNode();
            double laneDistance = (Double) lane.getProperty("distance");
            double travelDirection;
            double reverseTravelDirection;
            startTime = System.nanoTime();
            log.debug("Inside lane Calculation: " + index + " out of " + laneLoads.size());
            if(lane.hasProperty("travel_direction")) {
                travelDirection = (double) lane.getProperty("travel_direction");
            }
            else{
                travelDirection =  bearing((double) lane.getProperty("start_city_latitude"),
                        (double)lane.getProperty("start_city_longitude"),
                        (double)lane.getProperty("end_city_latitude"),
                        (double)lane.getProperty("end_city_longitude"
                        ));
                lane.setProperty("travel_direction", travelDirection);
            }
            if(lane.hasProperty("reverse_travel_direction")) {
                reverseTravelDirection = (double) lane.getProperty("reverse_travel_direction");
            }
            else{
                reverseTravelDirection =  bearing((double) lane.getProperty("end_city_latitude"),
                        (double)lane.getProperty("end_city_longitude"),
                        (double)lane.getProperty("start_city_latitude"),
                        (double)lane.getProperty("start_city_longitude"
                        ));
                lane.setProperty("reverse_travel_direction", reverseTravelDirection);
            }
            Traverser routeTraverse = routeTd.traverse(lpc);
            for(Path routePath : routeTraverse){
                Relationship cocRel = routePath.lastRelationship();
                double pathDistance = (double) cocRel.getProperty("distance");
                if( pathDistance > laneDistance * .50){
                    continue;
                }
                Node coc = cocRel.getOtherNode(lpc);
                Iterable<Relationship> cocLaneRels = coc.getRelationships(RelationshipType.withName(laneStartName), Direction.INCOMING);
                double finalTravelDirection = travelDirection;
                double finalReverseTravelDirection = reverseTravelDirection;
                cocLaneRels.forEach(cocLaneRel -> {
                    Node possibleOverlapLane = cocLaneRel.getStartNode();
                    if( pathDistance + (double) possibleOverlapLane.getProperty("distance") > laneDistance * 1.5 ){
                        return;
                    }
                    overlapLaneMap.put(possibleOverlapLane.getId(), possibleOverlapLane);
                    double overlapTravelDirection;
                    if(possibleOverlapLane.hasProperty("travel_direction")) {
                        overlapTravelDirection = (double) possibleOverlapLane.getProperty("travel_direction");
                    }
                    else{
                        overlapTravelDirection = bearing((double) possibleOverlapLane.getProperty("start_city_latitude"),
                                (double)possibleOverlapLane.getProperty("start_city_longitude"),
                                (double)possibleOverlapLane.getProperty("end_city_latitude"),
                                (double)possibleOverlapLane.getProperty("end_city_longitude"
                                ));
                        possibleOverlapLane.setProperty("travel_direction", overlapTravelDirection);
                    }
                    if(finalTravelDirection - 60 > overlapTravelDirection && finalTravelDirection + 60 < overlapTravelDirection){
                        LongArrayList sameSideOverlaps = sameSideOverlap.get(lane.getId());
                        if( sameSideOverlaps == null ){
                            sameSideOverlaps = new LongArrayList();
                        }
                        sameSideOverlaps.add(possibleOverlapLane.getId());
                        sameSideOverlap.put(lane.getId(), sameSideOverlaps);
                    }
                    else if(finalReverseTravelDirection - 60 > overlapTravelDirection && finalReverseTravelDirection + 60 < overlapTravelDirection){
                        LongArrayList circularOverlaps = circularOverlap.get(lane.getId());
                        if( circularOverlaps == null ){
                            circularOverlaps = new LongArrayList();
                        }
                        circularOverlaps.add(possibleOverlapLane.getId());
                        circularOverlap.put(lane.getId(), circularOverlaps);
                    }
                    else{
                        LongArrayList continuousRunOverlaps = circularOverlap.get(lane.getId());
                        if( continuousRunOverlaps == null ){
                            continuousRunOverlaps = new LongArrayList();
                        }
                        continuousRunOverlaps.add(possibleOverlapLane.getId());
                        continuousRunOverlap.put(lane.getId(), continuousRunOverlaps);
                    }
                });
            }
            endTime = System.nanoTime();
            log.debug("Completed lane Calculation: " + index + " out of " + laneLoads.size() + " in " + (endTime - startTime) + " ns");
        }

        //results Printer
        File laneSummaryfile = new File(filename);
        try {
            FileWriter laneSummaryfileOutput = new FileWriter(laneSummaryfile);
            CSVWriter writer = new CSVWriter(laneSummaryfileOutput);
            String[] header = {"lane_start_city", "lane_start_state", "lane_end_city", "lane_end_state", "same_side_lane_overlaps", "circular_lane_overlaps", "continuous_run_overlaps", "my_loads", "my_carriers", "total_partner_loads", "total_partner_carriers"};
            writer.writeNext(header);
            while (laneMap.iterator().hasNext()) {
                Node lane = laneMap.iterator().next().value;
                Long laneId = lane.getId();
                String[] data = {};
                int row = 0;
                data[row++] = (String) lane.getProperty("start_city");
                data[row++] = (String) lane.getProperty("start_state");
                data[row++] = (String) lane.getProperty("end_city");
                data[row++] = (String) lane.getProperty("end_state");
                data[row++] = Integer.toString(sameSideOverlap.get(laneId).size());
                data[row++] = Integer.toString(circularOverlap.get(laneId).size());
                data[row++] = Integer.toString(continuousRunOverlap.get(laneId).size());
                data[row++] = Integer.toString(laneLoads.get(laneId).size());
                LongArrayList ssOverlaps = sameSideOverlap.get(laneId);
            }
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return this;
    }

    public static double bearing(double lat1, double lon1, double lat2, double lon2){
        double longitude1 = lon1;
        double longitude2 = lon2;
        double latitude1 = Math.toRadians(lat1);
        double latitude2 = Math.toRadians(lat2);
        double longDiff= Math.toRadians(longitude2-longitude1);
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
