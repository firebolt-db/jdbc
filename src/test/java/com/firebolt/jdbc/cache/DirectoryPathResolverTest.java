package com.firebolt.jdbc.cache;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The tests are disabled since they interfere with the other test classes (tests are run in parallel and mockito creates file in the temp location,
// which we modify in these tests, so the other tests would randomly fail)
@Disabled
public class DirectoryPathResolverTest {

    private static String originalUserHome;
    private static String originalOsName;

    private DirectoryPathResolver directoryPathResolver = new DirectoryPathResolver();

    @BeforeAll
    static void setupClass() {
        originalUserHome = System.getProperty(DirectoryPathResolver.USER_HOME_PROPERTY);
        originalOsName = System.getProperty(DirectoryPathResolver.OS_NAME_PROPERTY);
    }

    @AfterAll
    static void tearDownClass() {
        // set back the original properties
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, originalUserHome);
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY, originalOsName);
    }

    @Test
    public void canCreateFireboltDirectoryForWindows() {
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"windows 11");
        System.setProperty(DirectoryPathResolver.TEMP_DIRECTORY_PROPERTY, "/temp");
        assertEquals("/temp/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    @SetEnvironmentVariable(key="TMPDIR", value="/temp/xx/1234")
    public void canCreateFireboltDirectoryForMac() {
       System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"mac osx 18");
       assertEquals("/temp/xx/1234/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    public void canCreateFireboltDirectoryForLinux() {
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"linux");
        assertEquals("/tmp/Users/testuser/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    @SetEnvironmentVariable(key="XDG_RUNTIME_DIR", value="/temp/xx/5678")
    public void canCreateFireboltDirectoryForLinuxWhenUserTempDirectoryIsSet() {
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"linux");
        assertEquals("/temp/xx/5678/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

}
