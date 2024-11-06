package dataBaseConection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DB_PostgresSQL {

    private static Connection instance;
    private static final String DATA_BASE_URL = System.getProperty("COSMOSDB_POSTGRES_URL");
    private static final String USER = System.getProperty("COSMOSDB_POSTGRES_USER");
    private static final String PWD = System.getProperty("COSMOSDB_POSTGRES_PASSWORD");

    public synchronized static Connection getConnection() throws SQLException {
        if (instance != null && !instance.isClosed()) {
            return instance;
        }

        instance = DriverManager.getConnection(DATA_BASE_URL, USER, PWD);
        return instance;
    }
}