package mysql.hu.edu;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PersonDAO {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;

	public void readDataBase() throws Exception {
		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");

			// Setup the connection with the DB
			// connect = DriverManager.getConnection("jdbc:mysql://localhost/hqiuDB?" + "user=hqiu&password=hqiu");
			connect = DriverManager.getConnection("jdbc:mysql://hqiudbins.cnanurdtuzz6.us-east-1.rds.amazonaws.com:"
					+ "3306/hqiuDB?" + "user=hqiu&password=hqiu890908");

			// Statements allow to issue SQL queries to the database
			statement = connect.createStatement();
			
			// Problem 3: Create the table
			preparedStatement = connect.prepareStatement("CREATE TABLE FLOWERS (id INT NOT NULL AUTO_INCREMENT, "
					+ "name VARCHAR(30) NOT NULL, height DOUBLE NOT NULL, description VARCHAR(128) NOT NULL, "
					+ "PRIMARY KEY (ID))");
			preparedStatement.executeUpdate();
			
			// Problem 3: Insert a few records
			preparedStatement = connect.prepareStatement("INSERT INTO FLOWERS (name, height, description) VALUES "
					+ "('Rose', 14.5,'A woody perennial of the genus Rosa.'), "
					+ "('Lilium', 16.7, 'A genus of herbaceous flowering plants.'), "
					+ "('Tulip', 18.6, 'A genus of perennial, bulbous plants.')");
			preparedStatement.executeUpdate();			

			// Problem 2 & 3: Result set get the result of the SQL query
			resultSet = statement.executeQuery("SELECT * FROM hqiuDB.FLOWERS");
			writeResultSet(resultSet);

			// PreparedStatements can use variables and are more efficient
			// Add more records.
			preparedStatement = connect.prepareStatement("INSERT INTO hqiuDB.FLOWERS VALUES (DEFAULT, ?, ?, ?)");
			// ("name, height, description");

			// Parameters start with 1
			preparedStatement.setString(1, "Orchid");
			preparedStatement.setDouble(2, 7.6);
			preparedStatement.setString(3, "A diverse and widespread family of flowering plants.");
			preparedStatement.executeUpdate();

			preparedStatement = connect.prepareStatement("INSERT INTO FLOWERS (name, height, description) VALUES "
					+ "('Daisy', 5.2,'An herbaceous perennial plant.')");
			preparedStatement.executeUpdate();

			preparedStatement = connect.prepareStatement("SELECT name, height, description FROM hqiuDB.FLOWERS");
			resultSet = preparedStatement.executeQuery();
			writeResultSet(resultSet);

			// Problem 3: Remove some records.
			preparedStatement = connect.prepareStatement("DELETE FROM hqiuDB.FLOWERS WHERE name= ?");
			preparedStatement.setString(1, "Orchid");
			preparedStatement.executeUpdate();

			preparedStatement = connect.prepareStatement("DELETE FROM hqiuDB.FLOWERS WHERE name='Daisy'");
			preparedStatement.executeUpdate();

			resultSet = statement.executeQuery("SELECT * FROM hqiuDB.FLOWERS");
			writeResultSet(resultSet);		
			
			writeMetaData(resultSet);

		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	private void writeMetaData(ResultSet resultSet) throws SQLException {
		//   Now get some metadata from the database
		// Result set get the result of the SQL query

		System.out.println("The columns in the table are: ");

		System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
		for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
			System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
		}
	}

	private void writeResultSet(ResultSet resultSet) throws SQLException {
		// ResultSet is initially before the first data set
		while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number
			// which starts at 1
			// e.g. resultSet.getSTring(2);
			String name = resultSet.getString("name");
			Double height = resultSet.getDouble("height");
			String description = resultSet.getString("description");
			System.out.println("Name: " + name);
			System.out.println("Height: " + height);
			System.out.println("Description: " + description);
		}
		System.out.println();
	}

	// You need to close the resultSet
	private void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {

		}
	}

} 