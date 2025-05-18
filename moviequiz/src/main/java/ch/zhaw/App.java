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
                // Send a ping to confirm a successful connection
                MongoDatabase movieDB = mongoClient.getDatabase("general");
                MongoCollection<Document> collection = movieDB.getCollection("movies");
                System.out.println("Found " + collection.countDocuments() + " movies");

                // Prompt user for an ingredient
                Scanner keyScan = new Scanner(System.in);
                System.out.println("Enter an year: ");
                String inputYear = keyScan.nextLine();

                // Convert the user input to a long
                long yearValue = Long.parseLong(inputYear);

                AggregateIterable<Document> movieCol = collection.aggregate(Arrays.asList(new Document("$match", 
                    new Document("year", yearValue)), 
                    new Document("$count", "moviecount")));
                
                System.out.println("Count for " + inputYear + ": " + movieCol.first().get("moviecount"));

                // Create the aggregation pipeline
                AggregateIterable<Document> genresOfYear = collection.aggregate(
                    Arrays.asList(
                        new Document("$match", new Document("year", yearValue)),
                        new Document("$unwind", "$genres"),
                        new Document("$group", new Document("_id", "$genres"))
                    )
                );

                ArrayList<Document> genresList= genresOfYear.into(new ArrayList<Document>());
                for (Integer i = 0; i < genresList.size(); i++) {
                    System.out.println((i + 1) + ": " + genresList.get(i).get("_id"));
                }

                System.out.println("Select a genre (1-" + genresList.size() + "):");
                Integer choice = keyScan.nextInt();

                String selectedGenre = genresList.get(choice - 1).getString("_id");
                System.out.println("Selected " + selectedGenre);

                // Randomly pick 1 movie from the selected year and genre
                AggregateIterable<Document> yearMovieCursor = collection.aggregate(Arrays.asList(
                    new Document("$match",
                        new Document("year", yearValue)
                        .append("genres", selectedGenre)),
                    new Document("$sample", new Document("size", 1))
                ));
                Document yearMovie = yearMovieCursor.first();

                // Randomly pick 2 movies that are 5-10 years older, same genre
                AggregateIterable<Document> olderMoviesCursor = collection.aggregate(Arrays.asList(
                    new Document("$match",
                        new Document("year", new Document("$gt", yearValue - 10)
                                            .append("$lt", yearValue - 5))
                        .append("genres", selectedGenre)),
                    new Document("$sample", new Document("size", 2))
                ));

                ArrayList<Document> olderMovies = new ArrayList<>();
                for (Document doc : olderMoviesCursor) {
                    olderMovies.add(doc);
                }

                if (yearMovie == null) {
                    System.out.println("No movie found for year " + yearValue + " in genre " + selectedGenre);
                    keyScan.close();
                    return;
                }
                if (olderMovies.size() < 2) {
                    System.out.println("Not enough older movies (5-10 years older) found in genre " + selectedGenre);
                    keyScan.close();
                    return;
                }

                ArrayList<Document> movies = new ArrayList<>();
                movies.add(yearMovie);
                movies.addAll(olderMovies);
                Collections.shuffle(movies);

                // Empty print for newline
                System.out.println();
                for (Integer i = 0; i < movies.size(); i++) {
                    Document m = movies.get(i);
                    System.out.println((i + 1) + ": " + m.getString("title"));
                }

                System.out.println("Which movie was released in " + yearValue + "?");
                Integer userGuess = keyScan.nextInt();

                if (userGuess < 1 || userGuess > movies.size()) {
                    System.out.println("Invalid choice.");
                    keyScan.close();
                    return;
                }

                Document guessedMovie = movies.get(userGuess - 1);
                Integer guessedYear = guessedMovie.getInteger("year");

                if (guessedYear == yearValue) {
                    System.out.println("Correct!");
                } else {
                    System.out.println("Wrong!");
                }

                for (Document movie : movies) {
                    String t = movie.getString("title");
                    Integer y = movie.getInteger("year");
                    System.out.println(t + " was released in " + y);
                }

            
                keyScan.close();
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }
    }
}
