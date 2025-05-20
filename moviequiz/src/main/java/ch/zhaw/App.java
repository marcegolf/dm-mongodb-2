package ch.zhaw;

import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

import io.github.cdimascio.dotenv.Dotenv;

public class App {
    public static void main(String[] args) {
        // Disable logging
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        lc.getLogger("org.mongodb.driver").setLevel(Level.OFF);

        Dotenv dotenv = Dotenv.load();
        String connectionString = dotenv.get("DB_URI");

        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();

        // Create a new client and connect to the server
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            try {
                MongoDatabase recipeDB = mongoClient.getDatabase("general");
                MongoCollection<Document> movieCol = recipeDB.getCollection("movies");
                System.out.println("Found " + movieCol.countDocuments() + " movies");

                // let the user select a year
                Scanner keyScan = new Scanner(System.in);
                System.out.print("Select a year\n> ");
                int year = keyScan.nextInt();

                AggregateIterable<Document> movieOfYear = movieCol.aggregate(
                        Arrays.asList(new Document("$match",
                                new Document("year", year)),
                                new Document("$count", "moviecount")));

                System.out.println("Count for " + year + ": " + movieOfYear.first().get("moviecount"));

                // genres of year
                AggregateIterable<Document> genresOfYear = movieCol.aggregate(Arrays.asList(new Document("$match",
                        new Document("year", year)),
                        new Document("$unwind",
                                new Document("path", "$genres")),
                        new Document("$group",
                                new Document("_id", "$genres"))));

                ArrayList<Document> genresList = genresOfYear.into(new ArrayList<Document>());
                for (int i = 0; i < genresList.size(); i++) {
                    System.out.println((i + 1) + ": " + genresList.get(i).get("_id"));
                }

                // let the user select a genre
                System.out.print("Select a genre (1-" + genresList.size() + ") \n> ");
                int gIndex = keyScan.nextInt();
                String genre = genresList.get(gIndex - 1).get("_id").toString();
                System.out.println("Selected " + genre);

                // pick one random movie
                AggregateIterable<Document> query1 = movieCol.aggregate(Arrays.asList(new Document("$match",
                        new Document("genres", genre)),
                        new Document("$match", new Document("year", year)),
                        new Document("$sample", new Document("size", 1L))));

                // pick two older random movies
                AggregateIterable<Document> query2 = movieCol.aggregate(Arrays.asList(new Document("$match",
                        new Document("genres", genre)),
                        new Document("$match", new Document("$and", Arrays.asList(
                                new Document("year", new Document("$gt", year - 10)),
                                new Document("year", new Document("$lt", year - 5))))),
                        new Document("$sample",
                                new Document("size", 2L))));
                // save to list, shuffle
                ArrayList<Document> quizList = query2.into(new ArrayList<Document>());
                quizList.add(query1.first());
                Collections.shuffle(quizList);

                // print titles
                for (int i = 0; i < quizList.size(); i++) {
                    Document d = quizList.get(i);
                    System.out.println((i + 1) + ": " + d.get("title").toString());
                }

                // run the quiz
                System.out.print("Which movie was not released in " + year + "?\n> ");
                int movieNo = keyScan.nextInt();
                if (quizList.get(movieNo - 1).get("year").toString().equals(Integer.toString(year))) {
                    System.out.println("Correct!");
                } else {
                    System.out.println("Wrong!");
                }
                for (Document d : quizList) {
                    System.out.println(d.get("title").toString() + " was released in " + d.get("year"));
                }

                keyScan.close();
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }
    }
}
