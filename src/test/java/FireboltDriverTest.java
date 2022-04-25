import java.sql.*;


public class FireboltDriverTest {
    static String FIREBOLT_APP = "api.dev.firebolt.io";

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.nanoTime();
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        checkCompressParam(args);
        checkWithAccountParam(args);
        basicConnectionTest(args);
        checkWithoutEngineeParameter(args);
        System.out.println("Execution time in seconds  : " + timeElapsed / 1000000000.0);
    }

    private static void checkCompressParam(String[] args) {
        try {
            Class.forName("io.firebolt.jdbc.FireboltDriver");
            Connection con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?compress=", args[2], args[3]);
            Statement stmt = con.createStatement();
            ResultSet resultSet2 = stmt.executeQuery(args[4]);
            System.out.println(resultSet2);
            ResultSetMetaData metaData = resultSet2.getMetaData();
            int columnCount2 = metaData.getColumnCount();

            System.out.println("Columns info:");
            for (int i = 1; i <= columnCount2; ++i) {
                System.out.println(metaData.getColumnClassName(i) + "  :  " + metaData.getColumnName(i) + "  :  " + metaData.getColumnTypeName(i) + "  :  " + metaData.getColumnType(i));
            }

            System.out.println("Columns data:");
            while (resultSet2.next()) {
                for (int i = 1; i <= columnCount2; ++i) {
                    System.out.print(resultSet2.getObject(i) + ",");
                }
                System.out.println();
            }
            System.out.println("------DONE Compress=1 Param-----");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Class.forName("io.firebolt.jdbc.FireboltDriver");
            Connection con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?compress=0", args[2], args[3]);
            Statement stmt = con.createStatement();
            ResultSet resultSet2 = stmt.executeQuery(args[4]);
            ResultSetMetaData metaData = resultSet2.getMetaData();
            int columnCount2 = metaData.getColumnCount();

            System.out.println("Columns info:");
            for (int i = 1; i <= columnCount2; ++i) {
                System.out.println(metaData.getColumnClassName(i) + "  :  " + metaData.getColumnName(i) + "  :  " + metaData.getColumnTypeName(i) + "  :  " + metaData.getColumnType(i));
            }

            System.out.println("Columns data:");
            while (resultSet2.next()) {
                for (int i = 1; i <= columnCount2; ++i) {
                    System.out.print(resultSet2.getObject(i) + ",");
                }
                System.out.println();
            }
            System.out.println("------DONE Compress=0 Param-----");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void checkWithoutEngineeParameter(String[] args) {
        try {
            Class.forName("io.firebolt.jdbc.FireboltDriver");
            Connection con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?ssl=true&use_standard_sql=1", args[2], args[3]);
            Statement stmt = con.createStatement();
            ResultSet resultSet = stmt.executeQuery(args[4]);
            System.out.println("-----DONE without Engine-----");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void checkWithAccountParam(String[] args) {

        try {
            Class.forName("io.firebolt.jdbc.FireboltDriver");
            Connection con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?ssl=true&use_standard_sql=1&account=firebolt", args[2], args[3]);
            Statement stmt = con.createStatement();
            ResultSet resultSet = stmt.executeQuery(args[4]);
            System.out.println("-----DONE with Account without Engine-----");

            con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?ssl=true&use_standard_sql=1&account=firebolt&engine=" + args[1], args[2], args[3]);
            stmt = con.createStatement();
            resultSet = stmt.executeQuery(args[4]);
            System.out.println("-----DONE with Account and engine-----");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void printResultSet(String header, ResultSet resultSet) throws SQLException {
        System.out.println();
        System.out.println();
        System.out.println(header);
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                System.out.print(resultSet.getObject(i) + ",");
            }
            System.out.println();
        }
    }

    private static void basicConnectionTest(String[] args) {
        try {
            Class.forName("io.firebolt.jdbc.FireboltDriver");
            Connection con = DriverManager.
                    getConnection("jdbc:firebolt://" + FIREBOLT_APP + "/" + args[0] + "?ssl=true&use_standard_sql=1&engine=" + args[1], args[2], args[3]);
            Statement stmt = con.createStatement();

            ResultSet showTablesResultSet = stmt.executeQuery("show tables");
            printResultSet("show tables", showTablesResultSet);


            ResultSet schemas = con.getMetaData().getSchemas(null, null);
            printResultSet("getSchemas", schemas);

            ResultSet tables = con.getMetaData().getTables(null, null, null, null);
            printResultSet("getTables", tables);

            ResultSet columns = con.getMetaData().getColumns(null, null, null, null);
            printResultSet("getColumns", columns);

            System.out.println("Created DB Connection....");

            ResultSet resultSet2 = stmt.executeQuery(args[4]);

            ResultSetMetaData metaData = resultSet2.getMetaData();
            int columnCount2 = metaData.getColumnCount();

            System.out.println("Columns info:");
            for (int i = 1; i <= columnCount2; ++i) {
                System.out.println(metaData.getColumnClassName(i) + "  :  " + metaData.getColumnName(i) + "  :  " + metaData.getColumnTypeName(i) + "  :  " + metaData.getColumnType(i));
            }

            System.out.println("Columns data:");
            while (resultSet2.next()) {
                for (int i = 1; i <= columnCount2; ++i) {
                    System.out.print(resultSet2.getObject(i) + ",");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}