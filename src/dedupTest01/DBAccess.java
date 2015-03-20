package dedupTest01;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;


public class DBAccess {

	private static DBAccess singletonInstance = null;
	private Connection connection = null;
	
	/**
	 * Singleton Constructor
	 */
	private DBAccess(){
		
	}
	
	public static DBAccess getInstance(){
		if (singletonInstance == null){
			singletonInstance = new DBAccess();
		}
		
		return singletonInstance;
	}
	
	
	public Connection getConnection(){
		if(connection == null){
			connectDB();
		}
		
		return connection;
	}
	
	
	private void connectDB(){

	    // load the sqlite-JDBC driver using the current class loader
		try {
			Class.forName("org.sqlite.JDBC");
			System.out.println("JDBC driver for DB access loaded.");
		} catch(ClassNotFoundException e){
			System.out.println("Error at loading JDBC driver");
		}
	    
	    System.out.println(System.getProperty("user.dir"));

//	    Connection connection = null;
	    try
	    {
	      // create a database connection
	      connection = DriverManager.getConnection("jdbc:sqlite:fileStorage.db");
	      Statement statement = connection.createStatement();
	      statement.setQueryTimeout(30);  // set timeout to 30 sec.
	      
	      
	      
//	      statement.executeUpdate("drop table if exists person");
//	      statement.executeUpdate("create table person (id integer, name string)");
//	      statement.executeUpdate("insert into person values(1, 'leo')");
//	      statement.executeUpdate("insert into person values(2, 'yui')");
//	      ResultSet rs = statement.executeQuery("select * from person");
//	      while(rs.next())
//	      {
//	        // read the result set
//	        System.out.println("name = " + rs.getString("name"));
//	        System.out.println("id = " + rs.getInt("id"));
//	      }
	    }
	    catch(SQLException e)
	    {
	      // if the error message is "out of memory", 
	      // it probably means no database file is found
	      System.err.println(e.getMessage());
	    }
	
	}
}
