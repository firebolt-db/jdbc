package com.firebolt.jdbc.cache;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    @SetEnvironmentVariable(key="APPDATA", value="/temp")
    public void canCreateFireboltDirectoryForWindows() {
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"windows 11");
        assertEquals("/temp/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    public void canCreateFireboltDirectoryForMac() {
       System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
       System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"mac osx 18");
       assertEquals("/Users/testuser/Library/Application Support/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    public void canCreateFireboltDirectoryForLinux() {
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"linux");
        assertEquals("/Users/testuser/.config/fireboltDriverJdbc", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

}
