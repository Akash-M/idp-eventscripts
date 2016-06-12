package tum.idp.bhs;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App implements Runnable
{
    static BufferedReader in ;  static int quit=0;

    public void run(){
        String msg = null;
        while(true){
            try{
                msg=in.readLine();
            }catch(Exception e){}

            if(msg.equals("Q")) {quit=1;break;}
        }
    }
    public static void main( String[] args ) throws Exception
    {

        /*
        * Steps
        * 1) Delete all documents from all collections
        * 2) Initialize all static data for carousels and central storage
        * 3) Add Events with flights
        *
        * */


        in=new BufferedReader(new InputStreamReader(System.in));

        Thread t1=new Thread(new App());
        t1.start();
        System.out.println("Connecting to the Mongo DataBase");
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("idpdb");


        /*
        * Get The Collection
        * */
        System.out.println("Retrieving all the collections");
        MongoCollection<Document> carouselCollection = database.getCollection("carousel");
        MongoCollection<Document> flightCollection = database.getCollection("flight");
        MongoCollection<Document> centralStorageCollection = database.getCollection("centralStorage");
        MongoCollection<Document> workerCollection = database.getCollection("worker");
        MongoCollection<Document> eventHandlingStartCollection = database.getCollection("eventHandlingStart");
        MongoCollection<Document> eventStorageDepletionCollection = database.getCollection("eventStorageDepletion");
        MongoCollection<Document> eventBaggageArrivalCollection = database.getCollection("eventBaggageArrival");
        MongoCollection<Document> eventHandlingEndCollection = database.getCollection("eventHandlingEnd");
        MongoCollection<Document> groundHandlerCollection = database.getCollection("groundHandler");




        /*
        * Add Central Storage
        * */
        /*
        System.out.println("Adding Central Storage Data");
        centralStorageCollection.insertOne(new Document("_id", 1)
                .append("maxCapacity", 100)
                .append("currentCapacity", 10));
        */


        /*
        * Adding Ground Handlers
        * */
        /*
        InsertClassDAO.insertGroundHandler(groundHandlerCollection, 0, "HawaHandler");
        InsertClassDAO.insertGroundHandler(groundHandlerCollection, 1, "AeroHandler");
        InsertClassDAO.insertGroundHandler(groundHandlerCollection, 2, "AirHandler");
        InsertClassDAO.insertGroundHandler(groundHandlerCollection, 3, "LuftHandler");
        */

        System.out.println( "Hello World!" );
        System.out.println("press Q THEN ENTER to terminate");
        System.out.println("Retrieving all the collections");


        while(true) {
            if (quit == 1) break;
            System.out.println("(Sim) simulate from start=0 until end=10");
            System.out.println("(Sim) simulate timeperiod t=0");
            System.out.println("Initializing workers");
             /*
        * Delete everything inside collections
        * */
            System.out.println("Cleaning up the collections");
            MongoCursor<Document> cursor = carouselCollection.find().iterator();
            while (cursor.hasNext()){
                carouselCollection.deleteOne(cursor.next());
            }

            cursor = flightCollection.find().iterator();
            while (cursor.hasNext()){
                flightCollection.deleteOne(cursor.next());
            }

            cursor = eventHandlingStartCollection.find().iterator();
            while (cursor.hasNext()){
                eventHandlingStartCollection.deleteOne(cursor.next());
            }

            cursor = eventStorageDepletionCollection.find().iterator();
            while (cursor.hasNext()){
                eventStorageDepletionCollection.deleteOne(cursor.next());
            }

            cursor = eventBaggageArrivalCollection.find().iterator();
            while (cursor.hasNext()){
                eventBaggageArrivalCollection.deleteOne(cursor.next());
            }

            cursor = eventHandlingEndCollection.find().iterator();
            while (cursor.hasNext()){
                eventHandlingEndCollection.deleteOne(cursor.next());
            }

            cursor = workerCollection.find().iterator();
            while (cursor.hasNext()){
                workerCollection.deleteOne(cursor.next());
            }

            for(int i =0; i< 160; i++) {
                InsertClassDAO.insertWorker(workerCollection, i, 0, 1, 1, 10, 1, 1, i%4);
            }

            t1.sleep(10000);

            /*collection.update(new BasicDBObject("_id", "jo"),
                    new BasicDBObject("$set", new BasicDBObject("phone", "5559874321")));*/

            System.out.println("(Sim) simulate from start=10 until end=22");

            System.out.println("(Sim) simulate timeperiod t=16");
            /*
                (Event t=16 SH) flight=4, carousel=13, tuple=(2,16,16,0)
                (Event t=16 SD) flight=4
                (Event t=16 -->o) worker=81, flight=4, carousel=13, no delay
                (Event t=16 -->o) worker=95, flight=4, carousel=13, no delay
            */
            ArrayList<Integer> workers = new ArrayList<Integer>();
            workers.add(81);
            workers.add(95);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection, flightCollection,
                    eventStorageDepletionCollection, 16, 13, 4, 2, 1,1, workers  );
            t1.sleep(10000);

            System.out.println("(Sim) simulate timeperiod t=17");
            /*
                (Event t=17 SH) flight=2, carousel=14, tuple=(1,17,17,0)
                (Event t=17 SD) flight=2
                (Event t=17 -->o) worker=21, flight=2, carousel=14, no delay
            */
            workers.clear();
            workers.add(21);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    17, 14, 2, 1, 2,1, workers);
            t1.sleep(10000);

            System.out.println("(Sim) simulate timeperiod t=18");
            /*
                (Event t=18 SH) flight=12, carousel=2, tuple=(1,18,18,0)
                (Event t=18 SH) flight=9, carousel=15, tuple=(1,18,18,0)
                (Event t=18 SD) flight=12
                (Event t=18 SD) flight=9
                (Event t=18 -->o) worker=102, flight=12, carousel=2, no delay
                (Event t=18 -->o) worker=149, flight=9, carousel=15, no delay
            */
            workers.clear();
            workers.add(102);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    18, 2, 12, 2, 2,1, workers);

            workers.clear();
            workers.add(149);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    18, 15, 9, 2, 1,1, workers);

            t1.sleep(10000);
            System.out.println("(Sim) simulate timeperiod t=20");

            /*
                (Event t=20 SH) flight=22, carousel=11, tuple=(1,20,20,0)
                (Event t=20 SH) flight=18, carousel=13, tuple=(1,20,20,0)
                (Event t=20 SH) flight=24, carousel=15, tuple=(1,20,20,0)
                (Event t=20 SH) flight=20, carousel=2, tuple=(1,20,20,0)
                (Event t=20 SH) flight=1028, carousel=20, tuple=(1,20,20,0)
                (Event t=20 SD) flight=22
                (Event t=20 SD) flight=18
                (Event t=20 SD) flight=24
                (Event t=20 SD) flight=20
                (Event t=20 SD) flight=1028
                (Event t=20 -->o) worker=47, flight=24, carousel=15, no delay
                (Event t=20 -->o) worker=91, flight=1028, carousel=20, no delay
                (Event t=20 -->o) worker=105, flight=22, carousel=11, no delay
                (Event t=20 -->o) worker=151, flight=20, carousel=2, no delay
                (Event t=20 -->o) worker=158, flight=18, carousel=13, no delay
           */

            workers.clear();
            workers.add(105);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    20, 11, 22, 3 , 2,1, workers);

            workers.clear();
            workers.add(158);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    20, 13, 18, 3 , 1,1, workers);

            workers.clear();
            workers.add(47);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    20, 15, 24, 3, 3,1, workers);

            workers.clear();
            workers.add(151);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    20, 2, 20, 3 , 4,1, workers);

            workers.clear();
            workers.add(91);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    20, 20, 1028, 4 , 4,1, workers);

            t1.sleep(10000);

            System.out.println("(Sim) simulate timeperiod t=21");

            /*
                (Event t=21 SH) flight=11, carousel=14, tuple=(1,21,21,0)
                (Event t=21 SD) flight=11
                (Event t=21 -->o) worker=41, flight=11, carousel=14, no delay
            */
            workers.clear();
            workers.add(41);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    21, 14, 11, 5 ,4, 1, workers);
            t1.sleep(10000);

            System.out.println("(Sim) simulate from start=22 until end=34");

            System.out.println("(Sim) simulate timeperiod t=22");

            /*
                (Event t=22 SH) flight=19, carousel=4, tuple=(1,22,22,0)
                (Event t=22 SH) flight=29, carousel=10, tuple=(1,22,22,0)
                (Event t=22 SH) flight=32, carousel=14, tuple=(1,22,22,0)
                (Event t=22 SH) flight=25, carousel=18, tuple=(1,22,22,0)
                (Event t=22 SH) flight=30, carousel=0, tuple=(1,22,22,0)
                (Event t=22 SD) flight=19
                (Event t=22 SD) flight=29
                (Event t=22 SD) flight=32
                (Event t=22 SD) flight=25
                (Event t=22 SD) flight=30
                (Event t=22 -->o) worker=0, flight=25, carousel=18, no delay
                (Event t=22 -->o) worker=1, flight=30, carousel=0, no delay
                (Event t=22 -->o) worker=6, flight=19, carousel=4, no delay
                (Event t=22 -->o) worker=84, flight=32, carousel=14, no delay
                (Event t=22 -->o) worker=92, flight=29, carousel=10, no delay
             */
            workers.clear();
            workers.add(6);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    22, 4, 19, 5 ,5, 1, workers);
            t1.sleep(10000);

            workers.clear();
            workers.add(92);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    22, 10, 29, 5 ,6, 1, workers);
            t1.sleep(10000);

            workers.clear();
            workers.add(84);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    22, 14, 32, 6 ,6, 1, workers);
            t1.sleep(10000);

            workers.clear();
            workers.add(0);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    22, 18, 25, 6 ,1, 1, workers);
            t1.sleep(10000);

            workers.clear();
            workers.add(1);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    22, 0, 30, 6 ,2, 1, workers);
            t1.sleep(10000);

            /*(Event t=22 ðŸ›„) flight=2
            (Flight) 4 bags --> carousel 14
            (Flight) pulled remaining 0, bagsOnCarousel=4
            (Flight) loaded remaining 4
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 14, 2, 4);

            /*
            (Event t=22 ðŸ›„) flight=4
            (Flight) 17 bags --> carousel 13
            (Flight) pulled remaining 0, bagsOnCarousel=17
            (Flight) loaded 16 bags, bagsOnCarousel=1
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 13, 4, 17);
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 13, 4, -16);

            /*
            (Event t=22 ðŸ›„) flight=14
            (Event t=22 ðŸ›„) flight=12
            (Flight) 1 bags --> carousel 2
            (Flight) pulled remaining 0, bagsOnCarousel=1
            (Flight) loaded remaining 1
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 2, 14, 2);
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 2, 12, 0);


            /*
            (Event t=22 ðŸ›„) flight=11
            (Flight) 6 bags --> carousel 14
            (Flight) pulled remaining 0, bagsOnCarousel=6
            (Flight) loaded remaining 6
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 14, 11, 6);


            /*
            (Event t=22 ðŸ›„) flight=19
            (Flight) 3 bags --> carousel 4
            (Flight) pulled remaining 0, bagsOnCarousel=3
            (Flight) loaded remaining 3
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 4, 19, 3);

            /*
            (Event t=22 ðŸ›„) flight=1028
            (Flight) 1 bags --> carousel 20
            (Flight) pulled remaining 0, bagsOnCarousel=1
            (Flight) loaded remaining 1
            */
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 20, 1028, 1);

            /*(Event t=22 ðŸ›„) flight=22
            (Flight) 2 bags --> carousel 11
            (Flight) pulled remaining 0, bagsOnCarousel=2
            (Flight) loaded remaining 2*/

            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 11, 22, 2);


            /*(Event t=22 ðŸ›„) flight=20
            (Flight) 2 bags --> carousel 2
            (Flight) pulled remaining 0, bagsOnCarousel=2
            (Flight) loaded remaining 2*/
            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    22, 2, 20, 2);
            t1.sleep(10000);


            System.out.println("(Sim) simulate timeperiod t=23");
            workers.clear();
            workers.add(44);
            InsertClassDAO.insertStartHandlingAndStorageDepletionEvents(eventHandlingStartCollection, carouselCollection,
                    flightCollection, eventStorageDepletionCollection,
                    23, 16, 14, 1 ,4, 1, workers);


            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    23, 14, 2, 3);

            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    23, 16, 4, 5);

            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    23, 15, 9, 3);

            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    23, 2, 20, 6);

            InsertClassDAO.insertEventBaggageArrivalEvents(eventBaggageArrivalCollection, carouselCollection,
                    23, 14, 32, 4);

            t1.sleep(50000);

        }
    }
}
