import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLConnection {

    private Connection conn;

    public MySQLConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
            conn = DriverManager.getConnection(DataBaseUtil.URL);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
