import java.sql.*;

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

    public void createTables() {
        if (conn == null) {
            System.err.println("DataBase connection failed");
        }
        try {
            Statement statement = conn.createStatement();

            // drop tables if exists
            String sql = "DROP TABLE IF EXISTS Customer";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS BillingAddress";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS ShippingAddress";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS PurchaseCart";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS Product";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS Orders";
            statement.executeUpdate(sql);

            // create new tables

            sql = "CREATE TABLE ShippingAddress ("
                    + "shippingID INTEGER PRIMARY KEY, "
                    + "address VARCHAR(100) NOT NULL, "
                    + "city VARCHAR(30) NOT NULL, "
                    + "state VARCHAR(30) NOT NULL, "
                    + "zip CHAR(5) NOT NULL, "
                    + "country VARCHAR(30) NOT NULL "
                    + ")";
            statement.executeUpdate(sql);

            sql = "CREATE TABLE BillingAddress ("
                    + "billingID INTEGER PRIMARY KEY, "
                    + "address VARCHAR(100) NOT NULL, "
                    + "city VARCHAR(30) NOT NULL, "
                    + "state VARCHAR(30) NOT NULL, "
                    + "zip CHAR(5) NOT NULL, "
                    + "country VARCHAR(30) NOT NULL "
                    + ")";
            statement.executeUpdate(sql);

            sql = "CREATE TABLE Product("
                    + "productID INTEGER PRIMARY KEY, "
                    + "productName VARCHAR(100) NOT NULL, "
                    + "price FLOAT NOT NULL, "
                    + "quantityInStock INTEGER NOT NULL "
                    + ")";
            statement.executeUpdate(sql);

            sql = "CREATE TABLE Orders ("
                    + "orderID INTEGER NOT NULL, "
                    + "productID INTEGER NOT NULL, "
                    //+ "customerID INTEGER NOT NULL, " // I do not think this is necessary?
                    + "shippingID INTEGER NOT NULL, "
                    + "billingID INTEGER NOT NULL, "
                    + "PRIMARY KEY (orderID, productID), "
                    + "FOREIGN KEY (shippingID) REFERENCES ShippingAddress(shippingID), "
                        // More constraint may apply
                    + "FOREIGN KEY (billingID) REFERENCES BillingAddress(billingID)"
                    // More constraint may apply
                    + ")";

            sql = "CREATE TABLE Customer ("
                    + "customerID INTEGER PRIMARY KEY, "
                    + "email VARCHAR(30) NOT NULL, "
                    + "psWord VARCHAR(30) NOT NULL, "
                    + "firstName VARCHAR(20), "
                    + "lastName VARCHAR(20), "
                    + "shippingID INTEGER NOT NULL, "
                    + "billingID INTEGER NOT NULL, "
                    + "cartID INTEGER NOT NULL, "
                    + "FOREIGN KEY (shippingID) REFERENCES ShippingAddress(shippingID), "
                            // More constraints may apply
                    + "FOREIGN KEY (billingID) REFERENCES BillingAddress(billingID), "
                            // More constraints may apply
                    + "FOREIGN KEY (cartID) REFERENCES PurchaseCart(cartID)"
                            // More constraints may apply
                    + ")";
            statement.executeUpdate(sql);
            System.out.print("Import done successfully");

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void populateData() {
        if (conn == null) {
            System.err.println("DataBase connection failed");
        }

        String sql = "INSERT INTO ShippingAddress VALUES (?,?,?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/product.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setString(2, data[1]);
                    statement.setString(3, data[2]);
                    statement.setString(4, data[3]);
                    statement.setString(5, data[4]);
                    statement.setString(6, data[5]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String sql = "INSERT INTO BillingAddress VALUES (?,?,?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/product.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setString(2, data[1]);
                    statement.setString(3, data[2]);
                    statement.setString(4, data[3]);
                    statement.setString(5, data[4]);
                    statement.setString(6, data[5]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
