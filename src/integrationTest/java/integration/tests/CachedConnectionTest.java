package integration.tests;

import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.CacheServiceProvider;
import com.firebolt.jdbc.cache.CacheType;
import com.firebolt.jdbc.cache.DirectoryPathResolver;
import com.firebolt.jdbc.cache.FileService;
import com.firebolt.jdbc.cache.FilenameGenerator;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.ClientSecretCacheKey;
import com.firebolt.jdbc.testutils.TestFixtures;
import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
public class CachedConnectionTest extends IntegrationTest {

    private String secondEngineName;
    private String secondDbName;

    @BeforeAll
    void beforeAll() throws SQLException {
        executeStatementFromFile("/statements/cached-connection/ddl.sql");

        // create the engine and db here rather than the ddl so we can add the prefix to the engine and db names
        String engine = System.getProperty("engine");
        String db = System.getProperty("db");

        secondEngineName = engine + "_second_engine";
        secondDbName = db + "_second_db";

        try (Connection connection = createConnection()) {
            connection.createStatement().execute("CREATE ENGINE IF NOT EXISTS "  + secondEngineName +";");
            // manually start the engine
            connection.createStatement().execute("START ENGINE "  + secondEngineName +";");
            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + secondDbName + ";");
        }

        // clean up the cache directory after the fact, since creating and starting the engine might take some time and we
        // are expecting the cache file not to be older than 2 minutes.
        DirectoryPathResolver directoryPathResolver = new DirectoryPathResolver();

        // remove all the files from the directory path resolver
        CacheService cacheService = CacheServiceProvider.getInstance().getCacheService(CacheType.DISK);
        CacheKey cacheKey = new ClientSecretCacheKey(ConnectionInfo.getInstance().getPrincipal(), ConnectionInfo.getInstance().getSecret(), ConnectionInfo.getInstance().getAccount());
        cacheService.remove(cacheKey);

        // allow the file to be deleted
        sleepForMillis(200);

        FilenameGenerator filenameGenerator = new FilenameGenerator();
        String expectedCacheFile = filenameGenerator.generate(cacheKey);
        File file = Paths.get(directoryPathResolver.resolveFireboltJdbcDirectory().toString(), expectedCacheFile).toFile();
        assertFalse(file.exists());

    }

    @AfterAll
    void afterAll() throws SQLException {
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("STOP ENGINE " + secondEngineName + " WITH TERMINATE=true;");
            connection.createStatement().execute("DROP ENGINE IF EXISTS " + secondEngineName+ ";");
            connection.createStatement().execute("DROP DATABASE IF EXISTS " + secondDbName + ";");
        }

        executeStatementFromFile("/statements/cached-connection/cleanup.sql");
    }

    @Test
    @Tag(TestTag.V2)
    @Tag(TestTag.SLOW)
    void createTwoConnections() throws SQLException {
        String testStartTime = getCurrentUTCTime();

        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 101");
            assertTrue(rs.next());
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(secondEngineName, secondDbName)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 102");
            assertTrue(rs.next());
        }

        // create a connection back on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 103");
            assertTrue(rs.next());

            // wait for the query history to propagate
            sleepForMillis(10000);

            // check that the connection cached was using the correct connection to the db and engine
            String queryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history " +
				"WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 101;' or query_text='SELECT 103;') " +
				"order by submitted_time desc";
            ResultSet engineOneResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 103;", engineOneResultSet.getString(1));

            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 101;", engineOneResultSet.getString(1));

            assertFalse(engineOneResultSet.next());

        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(secondEngineName, secondDbName)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 104");
            assertTrue(rs.next());

            // wait for the query history to propagate
            sleepForMillis(10000);

            // check that the connection cached was using the correct connection to the db and engine
            String queryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history " +
				"WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 102;' or query_text='SELECT 104;') " +
				"order by submitted_time desc";

            ResultSet engineTwoResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 104;", engineTwoResultSet.getString(1));

            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 102;", engineTwoResultSet.getString(1));

            assertFalse(engineTwoResultSet.next());
        }

        // verify cache file exists
        DirectoryPathResolver directoryPathResolver = new DirectoryPathResolver();
        Path fireboltDriverDirectory = directoryPathResolver.resolveFireboltJdbcDirectory();

        FilenameGenerator filenameGenerator = new FilenameGenerator();
        String expectedCacheFile = filenameGenerator.generate(new ClientSecretCacheKey(ConnectionInfo.getInstance().getPrincipal(), ConnectionInfo.getInstance().getSecret(), ConnectionInfo.getInstance().getAccount()));

        // the cache file exists
        File file = Paths.get(fireboltDriverDirectory.toString(), expectedCacheFile).toFile();
        assertTrue(file.exists());
    }

    @Test
    @Tag(TestTag.V2)
    void canDetectFileCacheCreationTime() throws SQLException {
        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 808");
            assertTrue(rs.next());
        }

        // sleep for some time to allow for the file to be created
        TestFixtures.sleepForMillis(200);

        DirectoryPathResolver directoryPathResolver = new DirectoryPathResolver();
        Path fireboltDriverDirectory = directoryPathResolver.resolveFireboltJdbcDirectory();

        FilenameGenerator filenameGenerator = new FilenameGenerator();
        String expectedCacheFile = filenameGenerator.generate(new ClientSecretCacheKey(ConnectionInfo.getInstance().getPrincipal(), ConnectionInfo.getInstance().getSecret(), ConnectionInfo.getInstance().getAccount()));

        File file = Paths.get(fireboltDriverDirectory.toString(), expectedCacheFile).toFile();
        FileService fileService = FileService.getInstance();

        assertTrue(file.exists(), "Did not find the cache file");
        assertFalse(fileService.wasFileCreatedBeforeTimestamp(file, 2, ChronoUnit.MINUTES));
        assertTrue(fileService.wasFileCreatedBeforeTimestamp(file, 1, ChronoUnit.MILLIS));
    }
}
