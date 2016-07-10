package tum.idp.bhs;

import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.bson.Document;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by prate_000 on 26-05-2016.
 */
public class InsertClassDAO {

    public static Boolean insertCarousel(MongoCollection<Document> carouselCollection, int id, int xCord,
                                  int yCord, int parkPos, int maxCap, int currCap,
                                  Integer[] flights, Integer workStations){

        BasicDBList flightDBList = new BasicDBList();
        for(int i=0; i < flights.length; i++){
            flightDBList.add(flights[i]);
        }

        if(carouselCollection.find(new Document("_id",id)).iterator().hasNext()){
            ArrayList currentFlightList = (ArrayList) carouselCollection.find(new Document("_id", id)).first().get("flight_id");
            for (Iterator i = currentFlightList.iterator();i.hasNext();)
                flightDBList.add(i.next());

            Document searchQuery = new Document().append("_id", id);
            Document updateQuery = new Document();
            updateQuery.append("$set", new Document().append("flight_id", flightDBList));
            carouselCollection.updateOne(searchQuery, updateQuery);
        }
        else {

            carouselCollection.insertOne(new Document("_id", id)
                    .append("xCoordinate", xCord)
                    .append("yCoordinate", yCord)
                    .append("parkingPositions", parkPos)
                    .append("maxCapacity", maxCap)
                    .append("currentCapacity", currCap)
                    .append("flight_id", flightDBList)
                    .append("workStations", workStations));
        }
        return true;

    }


    public static Boolean insertFlight(MongoCollection<Document> flightCollection,
                                int id, int requiredNumberOfParkingStations,
                                int expectedEndOfBaggageHandling,
                                int carousel_id,
                                ArrayList<Integer> worker_id, ArrayList<Integer> workStations){

        BasicDBList workerDBList = new BasicDBList();
        for(int i=0; i < worker_id.size(); i++){
            workerDBList.add(worker_id.get(i));
        }

        BasicDBList workingStationList = new BasicDBList();
        if (null != workStations){
            for(int i=0; i < workStations.size(); i++){
                workingStationList.add(workStations.get(i));
            }
        }

            /*

            {
                "_id" : 1,
                "requiredNumberOfParkingStations" : 10,
                "expectedEndOfBaggageHandling" : 22,
                "carousel_id" : 1,
                "worker_id" : [
                    1,
                    2
                ],
                "workstations" : [
                    1,
                    2
                ]
            }


                *
                * */
        flightCollection.insertOne(new Document("_id", id)
                .append("requiredNumberOfParkingStations", requiredNumberOfParkingStations)
                .append("expectedEndOfBaggageHandling", expectedEndOfBaggageHandling)
                .append("carousel_id", carousel_id)
                .append("worker_id", workerDBList )
                .append("workstations", workingStationList));

        return true;
    }

    public static Boolean insertWorker(MongoCollection<Document> workerCollection,
                                int id , int shiftStartTime,
                                int shiftStartXCoordinate, int shiftStartYCoordinate,
                                int shiftEndTime,int shiftEndXCoordinate, int shiftEndYCoordinate,
                                int groundHandler_id){

        /*
        *                   Collection: worker
                            {
                                "_id" : 1,
                                "shiftStartTime" : 1,
                                "shiftStartXCoordinate" : 1,
                                "shiftStartYCoordinate" : 1,
                                "shiftEndTime" : 1,
                                "shiftEndXCoordinate" : 1,
                                "shiftEndYCoordinate" : 1,
                                "groundHandler_id" : 1
                            }

        * *
        *
        * */
        workerCollection.insertOne(new Document("_id", id)
                .append("shiftStartTime", shiftStartTime)
                .append("shiftStartXCoordinate", shiftStartXCoordinate)
                .append("shiftStartYCoordinate", shiftStartYCoordinate)
                .append("shiftEndTime", shiftEndTime )
                .append("shiftEndXCoordinate", shiftEndXCoordinate)
                .append("shiftEndYCoordinate", shiftEndYCoordinate)
                .append("groundHandler_id", groundHandler_id));

        return true;
    }


    public static Boolean insertGroundHandler(MongoCollection<Document> groundHandlerCollection,
                                       int id, String Name){
        /*
        Collection: groundHandler
        { "_id" : 1, "_name" : "AeroHandler" }
        */

        groundHandlerCollection.insertOne(new Document("_id", id)
                .append("_name", Name));

        return true;

    }

    public static Boolean insertEventHandlingStart(MongoCollection<Document> eventHandlingStartCollection, int time , int carousel_id,
                                             int flight_id, int No_of_workstations){

        /*Collection: eventHandlingStart
        {
        "_id" : ObjectId("572211a05bf245a41c5cdd36"),
            "_time" : 1,
            "carousel_id" : 1,
            "flight_id" : 1,
            "workingstations" : 1
        }*/

        eventHandlingStartCollection.insertOne(new Document("_time",time)
                .append("carousel_id", carousel_id)
                .append("flight_id", flight_id)
                .append("workStations", No_of_workstations ));

        return true;

    }

    public static Boolean insertEventStorageDepletionStart(MongoCollection<Document> eventStorageDepletionStartCollection,
                                                    int time, int flight_id) {


        /*Collection: eventStorageDepletionStart
        {
            "_id" : ObjectId("572211a05bf245a41c5cdd38"),
                "_time" : 1,
                "flight_id" : 1
        }*/

        eventStorageDepletionStartCollection.insertOne(new Document("_time", time)
                .append("flight_id", flight_id));

        return true;


    }

    public static Boolean insertEventBaggageArrival (MongoCollection<Document> eventBaggageArrivalCollection,
                                                    int time, int flight_id, int bags) {


        eventBaggageArrivalCollection.insertOne(new Document("_time", time)
                .append("bags", bags)
                .append("flight_id", flight_id ));

        return true;

    }

    public static Boolean insertEventHandlingEnd (MongoCollection<Document> eventHandlingEndCollection,
                                              int time, int flight_id, int worker_id) {


        /*Collection: eventHandlingEnd
        {
            "_id" : ObjectId("572211a05bf245a41c5cdd39"),
                "_time" : 1,
                "flight_id" : 1,
                "worker_id" : 1
        }*/



        eventHandlingEndCollection.insertOne(new Document("_time", time)
                .append("flight_id", flight_id )
                .append("worker_id", worker_id));

        return true;

    }

    public static Boolean insertStartHandlingAndStorageDepletionEvents(MongoCollection<Document> eventHandlingStartCollection,
                                                                       MongoCollection<Document> carouselCollection,
                                                                       MongoCollection<Document> flightCollection,
                                                                       MongoCollection<Document> eventStorageDepletionCollection,
                                                                       int time, int carousel_id,
                                                                       int flight_id, int no_of_workingstations,
                                                                       int carousel_xCord, int carousel_yCord, ArrayList<Integer> worker_id){
        insertEventHandlingStart(eventHandlingStartCollection, time,carousel_id, flight_id, no_of_workingstations);
        Integer[] newflights = {flight_id};
        insertCarousel(carouselCollection, carousel_id, carousel_xCord, carousel_yCord, 1, 40, 0, newflights, no_of_workingstations );
        insertEventStorageDepletionStart(eventStorageDepletionCollection, time, flight_id);
        insertFlight(flightCollection, flight_id, 1, time+12,carousel_id, worker_id, null );

        return true;
    }

    public static Boolean insertEventBaggageArrivalEvents(MongoCollection<Document> eventBaggageArrivalCollection,
                                                          MongoCollection<Document> carouselCollection,
                                                          int time, int carousel_id, int flight_id, int bagsAddedOrRemoved){

        insertEventBaggageArrival(eventBaggageArrivalCollection, time, flight_id, bagsAddedOrRemoved);

        Document searchQuery = new Document().append("_id", carousel_id);
        Document updateQuery = new Document();
        updateQuery.append("$inc", new Document().append("currentCapacity", bagsAddedOrRemoved));
        carouselCollection.updateOne(searchQuery, updateQuery);
        return true;
    }

}
