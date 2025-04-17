import com.firebolt.FireboltDriver;
import com.firebolt.jdbc.connection.LocalhostFireboltConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class SmokeTest {

    public static void main(String[] args) {
        FireboltDriver fireboltDriver = new FireboltDriver();
        String url = createUrlConnectionString("goprean", "goprean");
        System.out.println("Connecting to localhost " + url);
        try (Connection connection = fireboltDriver.connect(url, new Properties())) {
            LocalhostFireboltConnection localhostFireboltConnection = connection.unwrap(LocalhostFireboltConnection.class);

            System.out.println("Successfully connected to localhost");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static String createUrlConnectionString(String database, String engine) {
        StringBuilder urlConnectionBuilder = new StringBuilder("jdbc:firebolt:")
                .append(database)
                .append("?engine=").append(engine)
                .append("&account=developer")
                .append("&host=localhost")
                .append("&access_token=some_token");
        return urlConnectionBuilder.toString();
    }

}