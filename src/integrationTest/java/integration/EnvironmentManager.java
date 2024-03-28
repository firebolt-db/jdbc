package integration;

import com.firebolt.FireboltDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class EnvironmentManager {
    private final String name = "instegration_testing_" + System.currentTimeMillis();
    private Connection connection;

    enum Action {
        CREATE, START, STOP, DROP
    }

    enum Entity {
        ENGINE("engine"), DATABASE("db");
        private final String property;

        Entity(String property) {
            this.property = property;
        }
    }

    public Properties create() throws SQLException, ClassNotFoundException {
        connection = createDbConnection();
        Properties props = new Properties();
        perform(props, Entity.ENGINE, name, Action.CREATE);
        perform(props, Entity.DATABASE, name, Action.CREATE);
        return props;
    }

    public void cleanup() throws SQLException {
        perform(System.getProperties(), Entity.ENGINE, name, Action.STOP, Action.DROP);
        perform(System.getProperties(), Entity.DATABASE, name, Action.DROP);
        connection.close();
    }

    private Connection createDbConnection() throws SQLException, ClassNotFoundException {
        Class.forName(FireboltDriver.class.getName());
        Properties props = new Properties();
        props.setProperty("env", System.getProperty("env", "staging"));
        props.setProperty("account", System.getProperty("account", "infra-engines-v2"));
        props.setProperty("client_id", System.getProperty("client_id", System.getProperty("user")));
        props.setProperty("client_secret", System.getProperty("client_secret", System.getProperty("password")));
        return DriverManager.getConnection("jdbc:firebolt:" + name, props);
    }

    private void perform(Properties props, Entity entity, String entityName, Action ... actions) throws SQLException {
        if (System.getProperty(entity.property) == null) {
            for (Action action : actions) {
                executeSql(String.format("%s %s %s", action, entity, entityName));
                if (Action.CREATE.equals(action)) {
                    props.setProperty(entity.property, entityName);
                }
            }
        }
    }

    private void executeSql(String sql) throws SQLException {
        connection.createStatement().executeUpdate(sql);
    }
}
