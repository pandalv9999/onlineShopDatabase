/**
 * Main
 */
public class Main {

    public static void main(String[] args) {
        MySQLConnection sql_conn = new MySQLConnection();
        sql_conn.createTables();
        sql_conn.populateData();
        sql_conn.createViews();
        sql_conn.displayViews();
    }
}