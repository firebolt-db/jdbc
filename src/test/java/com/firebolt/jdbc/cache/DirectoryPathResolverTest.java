package com.firebolt.jdbc.cache;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DirectoryPathResolverTest {

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
    void canCreateFireboltDirectoryForWindows() {
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"windows 11");
        System.setProperty(DirectoryPathResolver.TEMP_DIRECTORY_PROPERTY, "/temp");
        assertEquals("/temp/fireboltDriver", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    @SetEnvironmentVariable(key="TMPDIR", value="/temp/xx/1234")
    void canCreateFireboltDirectoryForMac() {
       System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"mac osx 18");
       assertEquals("/temp/xx/1234/fireboltDriver", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    @SetEnvironmentVariable(key="XDG_RUNTIME_DIR", value="")
    void canCreateFireboltDirectoryForLinux() {
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"linux");
        assertEquals("/tmp/Users/testuser/fireboltDriver", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

    @Test
    @SetEnvironmentVariable(key="XDG_RUNTIME_DIR", value="/temp/xx/5678")
    void canCreateFireboltDirectoryForLinuxWhenUserTempDirectoryIsSet() {
        System.setProperty(DirectoryPathResolver.USER_HOME_PROPERTY, "/Users/testuser");
        System.setProperty(DirectoryPathResolver.OS_NAME_PROPERTY,"linux");
        assertEquals("/temp/xx/5678/fireboltDriver", directoryPathResolver.resolveFireboltJdbcDirectory().toString());
    }

}
