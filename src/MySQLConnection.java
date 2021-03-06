
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class MySQLConnection {

    private Connection conn;

    public MySQLConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
            //Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
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

            String sql = "DROP TABLE IF EXISTS Product";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS Orders";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS Customer";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS BillingAddress";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS ShippingAddress";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS PurchaseCart";
            statement.executeUpdate(sql);

            sql = "DROP TABLE IF EXISTS OrderHistory";
            statement.executeUpdate(sql);

            // drop views
            sql = "DROP VIEW IF EXISTS TopSelling";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS TopBuyer";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS TotalPrice";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS ProductCertainDate";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS CustomerCart";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS OrderHistoryByCustomer";
            statement.executeUpdate(sql);

            sql = "DROP VIEW IF EXISTS ShoppingCartTotalPrice";
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
                    + "name VARCHAR(100) NOT NULL, "
                    + "price FLOAT NOT NULL, "
                    + "quantityInStock INTEGER NOT NULL "
                    + ")";
            statement.executeUpdate(sql);

            sql = "CREATE TABLE Orders ("
                    + "orderID INTEGER NOT NULL, "
                    + "productID INTEGER NOT NULL, "
                    + "shippingID INTEGER NOT NULL, "
                    + "billingID INTEGER NOT NULL, "
                    //Add orderDate column for the convenience to create view.
                    + "OrderDate DATE NOT NULL, "
                    + "quantity INTEGER NOT NULL,"

                    + "PRIMARY KEY (orderID, productID), "
                    + "FOREIGN KEY (shippingID) REFERENCES ShippingAddress(shippingID), "
                    // More constraint may apply
                    + "FOREIGN KEY (billingID) REFERENCES BillingAddress(billingID)"
                    // More constraint may apply
                    + ")";

            statement.executeUpdate(sql);

            sql = "CREATE TABLE PurchaseCart ("
                    + "cartID INTEGER, "
                    + "productID INTEGER, "
                    + "quantity INTEGER, "
                    + "PRIMARY KEY (cartID, productID)"
                    + ")";

            statement.executeUpdate(sql);

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

            // we need another table to link customer with his orders.
            sql = "CREATE TABLE OrderHistory ("
                    + "customerID INTEGER,"
                    + "OrderID INTEGER,"
                    + "PRIMARY KEY (customerID, OrderID)"
                    + ")";

            statement.executeUpdate(sql);
            System.out.println("Import done successfully");

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
            DataReader reader = new DataReader("data/ShippingAddress.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
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
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "INSERT INTO BillingAddress VALUES (?,?,?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/BillingAddress.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
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
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "INSERT INTO Product VALUES (?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/Product.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setString(2, data[1]);
                    statement.setFloat(3, Float.parseFloat(data[2]));
                    statement.setInt(4, Integer.parseInt(data[3]));
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "INSERT INTO PurchaseCart VALUES (?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/PurchaseCart.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setInt(2, Integer.parseInt(data[1]));
                    statement.setInt(3, Integer.parseInt(data[2]));
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // #id,email,psWord,firstName,lastName,shippingId,billingId,cartId
        sql = "INSERT INTO Customer VALUES (?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/Customer.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setString(2, data[1]);
                    statement.setString(3, data[2]);
                    statement.setString(4, data[3]);
                    statement.setString(5, data[4]);
                    statement.setInt(6, Integer.parseInt(data[5]));
                    statement.setInt(7, Integer.parseInt(data[6]));
                    statement.setInt(8, Integer.parseInt(data[7]));
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        sql = "INSERT INTO OrderHistory VALUES (?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/OrderHistory.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setInt(2, Integer.parseInt(data[1]));
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // other tables
        // # orderID,productID,shippingID,billingID,orderDate,quantity
        sql = "INSERT INTO Orders VALUES (?,?,?,?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/Orders.txt");
            String line;
            while ((line = reader.readLines()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] data = line.split(",");
                try {
                    statement.setInt(1, Integer.parseInt(data[0]));
                    statement.setInt(2, Integer.parseInt(data[1]));
                    statement.setInt(3, Integer.parseInt(data[2]));
                    statement.setInt(4, Integer.parseInt(data[3]));
                    statement.setString(5, data[4]);
                    statement.setInt(6, Integer.parseInt(data[5]));
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public void createViews() {
        try {
            Statement statement = conn.createStatement();

            //Top selling products, output top 10
            String topSelling = "CREATE VIEW TopSelling AS " +
                    "SELECT SUM(PurchaseCart.quantity) AS totalQuantity, Orders.productID, Product.name " +
                    "FROM Orders, PurchaseCart, Product " +
                    "WHERE Orders.productID = PurchaseCart.productID AND Orders.productID = Product.productID " +
                    "GROUP BY Orders.productID " +
                    "ORDER BY totalQuantity DESC; ";
            statement.executeUpdate(topSelling);

            //Top buyers
            String topBuyer = "CREATE VIEW TopBuyer AS " +
                    "SELECT COUNT(orderID) AS totalOrder, Customer.customerID, Customer.firstName, Customer.lastName " +
                    "FROM Customer, OrderHistory " +
                    "WHERE Customer.customerID = OrderHistory.customerID " +
                    "GROUP BY Customer.customerID " +
                    "Order BY totalOrder DESC; ";
            statement.executeUpdate(topBuyer);

            //Order History (a customer can view the total price of product in shopping cart)
            String totalPrice = "CREATE VIEW TotalPrice AS  " +
                    "SELECT SUM(Product.price * PurchaseCart.quantity) AS totalPrice, Customer.firstName, " +
                    "       Customer.lastName, Customer.CustomerID " +
                    "FROM Product, PurchaseCart, Customer " +
                    "WHERE Product.productID = PurchaseCart.productID AND Customer.cartID = PurchaseCart.cartID " +
                    "ORDER BY totalPrice DESC;";
            statement.executeUpdate(totalPrice);


            //Product sold on a particular date
            Scanner input = new Scanner(System.in);
            System.out.print("Enter a date (YYYY-MM-DD) that the products sold on: ");
            String certainDate = input.next();

            String productCertainDate = "CREATE VIEW ProductCertainDate AS " +
                    "SELECT Product.productID, Product.name " +
                    "FROM Orders, Product " +
                    "WHERE OrderDate = ? AND Orders.productID = Product.productID;";
            PreparedStatement st = conn.prepareStatement(productCertainDate);
            st.setDate(1, Date.valueOf(certainDate));
            st.executeUpdate();

            //display the shopping cart for certain customer
            System.out.println("Display the shopping cart for certain customer:");
            System.out.println("Enter a customer's first name:");
            String firstName = input.next();
            System.out.println("Enter a customer's last name:");
            String lastName = input.next();

            String customerCart = "CREATE VIEW CustomerCart AS " +
                    "SELECT PurchaseCart.*, Customer.firstName, Customer.lastName " +
                    "FROM PurchaseCart, Customer " +
                    "WHERE PurchaseCart.cartID = Customer.cartID AND Customer.firstName = ? AND Customer.lastName = ?;";
            st = conn.prepareStatement(customerCart);
            st.setString(1, firstName);
            st.setString(2, lastName);
            st.executeUpdate();


            // Order History (what has the customer ordered so far, for customer to review history)
            System.out.println("Display the order history by the customer:");
            String orderHistoryByCustomer = "CREATE VIEW OrderHistoryByCustomer AS  " +
                    "SELECT Customer.firstName, Customer.lastName, Orders.orderDate, Product.name " +
                    "FROM Product, Customer, OrderHistory, Orders " +
                    "WHERE Customer.firstName = ? AND Customer.lastName = ? " +
                    "AND OrderHistory.orderID = Orders.orderID AND OrderHistory.customerID = Customer.customerID " +
                    "AND Orders.productID = Product.productID;";
            st = conn.prepareStatement(orderHistoryByCustomer);
            st.setString(1, firstName);
            st.setString(2, lastName);
            st.executeUpdate();

            // Shopping cart total price by a certain customer (a customer can view the total price of product in shopping cart, for check out purpose)
            System.out.println("Display the shopping cart total price by the customer:");
            String shoppingCartTotalPrice = "CREATE VIEW ShoppingCartTotalPrice AS " +
                    "SELECT SUM(PurchaseCart.quantity * Product.price) AS cartTotalPrice, Customer.firstName, Customer.lastName, PurchaseCart.cartID " +
                    "FROM Customer, PurchaseCart, Product " +
                    "WHERE Customer.firstName = ? AND Customer.lastName = ? " +
                    "AND PurchaseCart.cartID = Customer.cartID AND Product.productID = PurchaseCart.productID " +
                    "GROUP BY PurchaseCart.cartID;";
            st = conn.prepareStatement(shoppingCartTotalPrice);
            st.setString(1, firstName);
            st.setString(2, lastName);
            st.executeUpdate();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void displayViews() {
        try {
            Statement statement = conn.createStatement();
            String view1 = "SELECT * FROM TopSelling " +
                    "LIMIT 10;";
            System.out.println("1. Display top 10 selling products:");
            ResultSet rs1 = statement.executeQuery(view1);
            printResult(rs1);

            String view2 = "SELECT * FROM TopBuyer " +
                    "LIMIT 10;";
            System.out.println("2. Display top 10 buyers:");
            ResultSet rs2 = statement.executeQuery(view2);
            printResult(rs2);

            String view3 = "SELECT * FROM TotalPrice ;";
            System.out.println("3: Order History (a customer can view the total price of product in shopping cart):");
            ResultSet rs3 = statement.executeQuery(view3);
            printResult(rs3);

            String view4 = "SELECT * FROM ProductCertainDate ;";
            System.out.println("4: Product sold on a particular date:");
            ResultSet rs4 = statement.executeQuery(view4);
            printResult(rs4);

            String view5 = "SELECT * FROM CustomerCart;";
            System.out.println("5: Display the customer's cart:");
            ResultSet rs5 = statement.executeQuery(view5);
            printResult(rs5);

            String view6 = "SELECT * FROM OrderHistoryByCustomer;";
            System.out.println("6: Display the customer's order history");
            ResultSet rs6 = statement.executeQuery(view6);
            printResult(rs6);

            String view7 = "SELECT * FROM ShoppingCartTotalPrice;";
            System.out.println("7: Display the total price of the shopping cart by a certain customer:");
            ResultSet rs7 = statement.executeQuery(view7);
            printResult(rs7);


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getMostSoldProduct() {
        if (conn == null) {
            System.err.println("DataBase connection failed");
        }
        try {
            Statement statement = conn.createStatement();
            
            String sql = "SELECT MAX(sum) FROM (SELECT ProductID, SUM(quantity) sum FROM Orders GROUP BY ProductID) T";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            int maxSoldNum = rs.getInt(1);
            System.out.println("\n Most popular product sold " +  maxSoldNum + " units, product details:");
            
            sql = "SELECT * FROM Product WHERE ProductID IN (SELECT ProductID FROM (SELECT ProductID, SUM(quantity) sum FROM Orders GROUP BY ProductID) T WHERE sum = " 
                + maxSoldNum + ")";
            rs = statement.executeQuery(sql);
            printResult(rs);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void printResult(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnsNumber = metaData.getColumnCount();
        System.out.println("-----------------");
        for (int i = 1; i <= columnsNumber; i++) {
            if (i == 1)
                System.out.print(" | ");
            System.out.print(metaData.getColumnName(i) + "   |   ");
        }
        System.out.println("\n-----------------");

        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i == 1)
                    System.out.print(" | ");
                System.out.print(rs.getString(i) + "   |   ");
            }
            System.out.println("");
        }
        System.out.println("");
    }

    public void close() throws SQLException {
        Statement statement = conn.createStatement();
        try {
            conn.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
