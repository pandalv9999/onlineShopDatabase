import java.sql.*;
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

            sql = "DROP TABLE IF EXISTS Cart";
            statement.executeUpdate(sql);



            sql = "DROP TABLE IF EXISTS OrderHistory";
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

            sql = "CREATE TABLE Cart ("
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
                    + "FOREIGN KEY (cartID) REFERENCES Cart(cartID)"
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
            DataReader reader = new DataReader("data/shippingAddress.txt");
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
            DataReader reader = new DataReader("data/billingAddress.txt");
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
            DataReader reader = new DataReader("data/product.txt");
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

        sql = "INSERT INTO Cart VALUES (?,?,?)";
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            DataReader reader = new DataReader("data/Cart.txt");
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
            DataReader reader = new DataReader("data/orderHistory.txt");
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
        try{
            Statement statement = conn.createStatement();
            //Top selling products, output top 10
            String topSellingProducts="CREATE VIEW topSellingProducts AS " +
                    "SELECT SUM(PurchaseCart.quantity) AS totalQuantity, Order.ProductID, Product.name " +
                    "FROM Order, PurchaseCart, Product " +
                    "WHERE Order.ProductID = PurchaseCart.ProductID AND Order.ProductID = Product.ID " +
                    "GROUP BY Order.ProductID " +
                    "ORDER BY totalQuantities DESC; " ;
            statement.executeUpdate(topSellingProducts);

            //Top buyers
            String topBuyer="CREATE VIEW topBuyer AS " +
                    "SELECT COUNT(Customer.orderID) AS totalOrder, Customer.name " +
                    "FROM Customer, Order " +
                    "WHERE Customer.orderID = Order.orderID " +
                    "GROUP BY Customer.orderID " +
                    "Order BY totalOrder DESC; " ;
            statement.executeUpdate(topBuyer);

            //Order History (a customer can view the total price of product in shopping cart)
            String orderHistory="CREATE VIEW orderHistory AS  " +
                    "SELECT SUM(Product.price*PurchaseCart.quantity) AS totalPrice, Customer.name AS customerName, Customer.CustomerID " +
                    "FROM Product, PurchaseCart, Customer " +
                    "WHERE Product.ProductID=PurchaseCart.ProductID AND Customer.cartID=PurchaseCart.cartID " +
                    "ORDER BY totalPrice DESC;";
            statement.executeUpdate(orderHistory);


            //Product sold on a particular date
            Scanner input = new Scanner(System.in);
            System.out.print("Enter a date (YYYY-MM-DD): ");
            String certainDate = input.next();

            String productCertainDate="CREATE VIEW productCertainDate AS " +
                    "SELECT Order.ProductID, Product.name " +
                    " FROM Order " +
                    "WHERE OrderDate = ? ;";
            PreparedStatement st= conn.prepareStatement(productCertainDate);
            st.setString(1, certainDate);
            st.executeUpdate(productCertainDate);

            //display the shopping cart for certain customer
            System.out.println("Enter a customer's first name:");
            String userFirstName = input.next();
            System.out.print("Enter a customer's last name:");
            String userLastName = input.next();

            String customerCart = "CREATE VIEW customerCart AS " +
                    "SELECT PurchaseCart.*, Customer.firstName, Customer.lastName " +
                    "FROM PurchaseCart, Customer " +
                    "WHERE PurchaseCart.CustomerID=Customer.CustomerID AND Customer.firstName= ? AND Customer.lastName= ?;";
            st = conn.prepareStatement(customerCart);
            st.setString(1, userFirstName);
            st.setString(2, userLastName);
            st.executeUpdate(customerCart);

        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void displayViews() {
        try{
            Statement statement = conn.createStatement();
            String view1="SELECT * FROM topSellingProducts " +
                    "LIMIT 10;";
            System.out.println("1. Display top 10 selling products:");
            ResultSet rs1 = statement.executeQuery(view1);
            printResult(rs1);

            String view2="SELECT * FROM topBuyer " +
                    "LIMIT 10;";
            System.out.println("2. Display top 10 buyers:");
            ResultSet rs2 = statement.executeQuery(view2);
            printResult(rs2);

            String view3="SELECT * FROM orderHistory ;";
            System.out.println("3: Order History (a customer can view the total price of product in shopping cart):");
            ResultSet rs3 = statement.executeQuery(view3);
            printResult(rs3);

            String view4="SELECT * FROM productCertainDate ;";
            System.out.println("4: Product sold on a particular date:");
            ResultSet rs4 = statement.executeQuery(view4);
            printResult(rs4);

            String view5 = "SELECT * FROM customerCart;";
            System.out.println("5: Display the customer's cart:");
            ResultSet rs5 = statement.executeQuery(view5);
            printResult(rs5);


        }catch (SQLException e) {
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
