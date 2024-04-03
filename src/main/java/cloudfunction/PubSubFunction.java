package cloudfunction;

import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.cloudevents.CloudEvent;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.logging.Logger;

public class PubSubFunction implements CloudEventsFunction {

    private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());
    private static final String DOMAIN_NAME = "divyashree.me";
    private static final String API_KEY = "57d674b8c2a168eec143337c005d2be7-f68a26c9-9a0ac153";
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");
    private static final String DB_NAME = System.getenv("DB_NAME");
    private static final String INSTANCE_HOST = System.getenv("INSTANCE_HOST");
    private static final String DB_PORT = System.getenv("DB_PORT");


    @Override
    public void accept(CloudEvent event) throws Exception {
        String cloudEventData = new String(event.getData().toBytes());
        Gson gson = new Gson();
        MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
        Message message = data.getMessage();
        String encodedData = message.getData();
        String decodedData = new String(Base64.getDecoder().decode(encodedData));
        logger.info("Pub/Sub message: " + decodedData);

        JSONObject jsonObject = new JSONObject(decodedData);
        String email = jsonObject.getString("email");
        logger.info("Json data Email :" + email);

        String verificationToken = Base64.getEncoder().encodeToString(email.getBytes());

        //generate VerificationLink
        Instant expirationTime = Instant.now().plus(Duration.ofMinutes(2));
        String verificationLink = "https://divyashree.me/verify?token=" + verificationToken
                + "&expires=" + expirationTime.toEpochMilli();

        String emailBody = "Click on the link to verify your email: " + verificationLink;
        logger.info("Email body is : " + emailBody);
        JsonNode response = null;

        //send Email
        try {
            HttpResponse<JsonNode> request = Unirest.post("https://api.mailgun.net/v3/" + DOMAIN_NAME + "/messages")
                    .basicAuth("api", API_KEY)
                    .queryString("from", "Divyashree <mailgun@" + DOMAIN_NAME + ">")
                    .queryString("to", email)
                    .queryString("subject", "Email Verification")
                    .queryString("text", emailBody)
                    .asJson();
            response = request.getBody();

            logger.info("Email sent " + response.getObject().toString());
        } catch(UnirestException e) {
            logger.severe("Email not sent " + e.getMessage());
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("UTC"));
        String formattedExpirationTime = formatter.format(expirationTime);

        String query = "UPDATE user SET email_body = ?, verification_link = ?, expiry_time = ? WHERE username = ?";
        try(Connection connection = getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, response.getObject().toString());
            stmt.setString(2, verificationLink);
            stmt.setString(3, formattedExpirationTime);
            stmt.setString(4, email);
            int row =  stmt.executeUpdate();
            logger.info("Rows updated: " + row);
        } catch (SQLException e) {
            logger.severe("Error saving to database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //jdbc:mysql://${google_sql_database_instance.mysql_instance[instance_key].private_ip_address}:3306/${google_sql_database.mysql_db[instance_key].name}
            String db_url = "jdbc:mysql://" + INSTANCE_HOST + ":" + DB_PORT + "/" + DB_NAME;
            conn = DriverManager.getConnection(db_url, DB_USER, DB_PASS);
            logger.info("Database connection successful.");
        } catch (ClassNotFoundException e) {
            logger.severe("MySQL JDBC Driver not found." + e.getMessage());
        } catch (SQLException e) {
            logger.severe("Database connection failed." + e.getMessage());
        }
        return conn;
    }
}