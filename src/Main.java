import java.sql.SQLException;

/**
 * Main
 */
public class Main {

    public static void main(String[] args) throws SQLException {
        MySQLConnection sql_conn = new MySQLConnection();
        sql_conn.createTables();
        sql_conn.populateData();
        sql_conn.createViews();
        sql_conn.displayViews();
        sql_conn.close();
    }
}