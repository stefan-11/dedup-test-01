package dedupTest01;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ParameterMetaData;
import java.io.*;
import java.util.*;

public class FileStorage {

	private static FileStorage singleton = null;
	private Connection connection = null;
	
	private FileStorage(){
		//connect to data base
		connectDB();
	}
	
	public static FileStorage getInstance(){
		if (singleton == null){
			singleton = new FileStorage();
		}
		
		return singleton;
	}
	
	public void connectDB(){
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
	
	public void closeDB(){
	      try
	      {
	        if(connection != null)
	          connection.close();
	      }
	      catch(SQLException e)
	      {
	        // connection close failed.
	        System.err.println(e);
	      }
	}
	
	/**
	 * Store given FileVersion in FileStorage (DB).
	 * 
	 * @param fileVersion
	 */
	public boolean storeFileVersion(FileVersion fileVersion){
		System.out.println("storing FileVersion...");
		
		String sql = null;
		
		try {
			
			//if the FileVersion doesn't exist in DB yet a new Version needs to be created
			if (fileVersion.existsInDB() == true){
				//FileVersion already exists in DB
				//...do nothing here
				System.out.println("FileVersion already exists in DB...do nothing.");
				return false;
				
			} else {
				//FileVersion doesn't exist in DB yet
				//find out the latest version
				sql = "select FileVersionId, max(Version) as maxVersion, FileVersionHash from FileVersion where Path = ? and Filename = ?";
				System.out.println("sql: " + sql);
				PreparedStatement stmt = connection.prepareStatement(sql);
				stmt.setString(1, fileVersion.getPath());
				stmt.setString(2, fileVersion.getFilename());
				
				ResultSet rs = stmt.executeQuery();
				rs.next();
				int fileVersionId = rs.getInt("FileVersionId");
//				System.out.println("new FileVersionId: " + fileVersionId);
				int maxVersion = rs.getInt("maxVersion");
				String maxVersionHash = rs.getString("FileVersionHash");
				System.out.println("maxVersion: " + maxVersion);
				
				//check if the latest version has the same fileVersionHash
				System.out.println("maxVersionHash:   " + maxVersionHash);
				System.out.println("fileVersion Hash: " + fileVersion.getFileVersionHash());
				if (fileVersion.getFileVersionHash().equals( maxVersionHash )){
					//hashes are equal...the current file is not different to the last version
					System.out.println("Hashes of last version and current fileVersion are equal. New fileVersion not saved.");
					
					fileVersion.setFileVersionId(fileVersionId);
					
					return false;
				} else {
					//the latest Version and current fileVersion have different hashes...so save the new one
					//inc maxVersion and set it into fileVersion
					fileVersion.setVersion(maxVersion+1);
					
					//insert the FileVersion record into the DB Table
					sql = "insert into FileVersion values (NULL, ?, ?, ?, 'CREATED', ?, NULL, NULL, NULL, NULL)";
					System.out.println("sql: " + sql);
					stmt = connection.prepareStatement(sql);	
					stmt.setString(1, fileVersion.getPath());
					stmt.setString(2, fileVersion.getFilename());
					stmt.setInt(3, fileVersion.getVersion());
					
//					System.out.println("FileVersionHash: " + fileVersion.getFileVersionHash());
					
					if (fileVersion.getFileVersionHash() != null){
						stmt.setString(4, fileVersion.getFileVersionHash());
					}
					
					stmt.executeUpdate();	
				
					//get the last record id and update the fileVersion
					sql = "select FileVersionId from FileVersion where OID = last_insert_rowid();";
					stmt = connection.prepareStatement(sql);	
					rs = stmt.executeQuery();
					rs.next();
					fileVersionId = rs.getInt("FileVersionId");
					System.out.println("fileVersionId: " + fileVersionId);
					
					fileVersion.setFileVersionId(fileVersionId);
					fileVersion.setExistsInDB(true);
					
					return true;
				}
			
			}
			

			
		} catch (Exception e){
			System.err.println(e);
			return false;
		}
		
	} //eom: storeFileVersion
	
	public void storeFileChunksForFileVersion(FileVersion fileVersion){
		
		//check that the physical file is actually the one we have in fileVersion
		System.out.println("TODO: check if physical file is same as the file mentioned by fileVersion.");
		
		//open the file for reading
		String filenameAndPath = fileVersion.getPath() + fileVersion.getFilename();
	    File aFile = new File(filenameAndPath);
	    FileInputStream inFile = null;
	    
	    try {
	    	inFile = new FileInputStream(aFile);

		    FileChannel inChannel = inFile.getChannel();
		    ByteBuffer buf = ByteBuffer.allocate(1024);
		    
		    int sequenceId = 0;
		    
		    while (inChannel.read(buf) != -1) {
//		    	System.out.println("reading sequenceId " + sequenceId);
		    	buf.flip();
		    	byte[] byteArray = buf.array();
//		    	String test = new String(byteArray);
//		    	System.out.println("test: " + test);
//		    	System.out.println("String read: " + ((ByteBuffer) (buf.flip())).asCharBuffer().get(0));
		    	
//		    	DataChunk chunk = new DataChunk(byteArray);
//		    	this.storeFileChunk(fileVersion, sequenceId, chunk);
		    	
		    	buf.clear();
		    	
		    	sequenceId++;
		    }
		    inFile.close();

	    } catch (FileNotFoundException e1){
	    	System.out.println("Error: File not found in FileStorage.storeFileChunksForFileVersions");
	    	e1.printStackTrace();
	    } catch (IOException e2){
	    	System.out.println("Error: Error reading file in FileStorage.storeFileChunksForFileVersions");
	    	e2.printStackTrace();
	    }
	    
	}
	
//	/**
//	 * Stores a given FileChunk in DB or creates a pointer if the FileChunk already exists in DB
//	 * 
//	 * @param chunk
//	 * @return 1 if the fileChunk was created in DB
//	 * 		   2 if the fileChunk was not created (in this case only a pointer is created). So false 2 not mean an error.
//	 * 		   -1 an error occured and the chunk could not be stored
//	 */
//	public int storeFileChunk(FileVersion fileVersion, int sequenceId, DataChunk chunk){
////		System.out.println("storing FileChunk...");
//		
//		boolean chunkCreated = false;
//		boolean pointerCreated = false;
//		
//		int selectedDataChunkId = 0;
//		
//		try {
//		
//			//check if the FileChunk already exists in DB
//			String sql = "select DataChunkId from DataChunk where Hash = ?";
//			PreparedStatement stmt = connection.prepareStatement(sql);
//			
//			stmt.setBytes(1, chunk.getHash());
//			
//			ResultSet rs = stmt.executeQuery();
//			
//			rs.next();
//			selectedDataChunkId = rs.getInt("DataChunkId");
//			System.out.println("DataChunk already exists (FileChunkId: " + selectedDataChunkId + ")");	
//			
//			chunk.setFileChunkId(selectedDataChunkId);
//			
//		} catch (SQLException e1){
////			System.err.println(e1);
//			
//			//no result found in DB...so create file chunk
//			System.out.println("creating DataChunk in FileStorage...");
//			
//			try {
//				//create the chunk in the DB
//				String sql = "insert into DataChunk values (NULL, ?, ?)";
//				PreparedStatement stmt = connection.prepareStatement(sql);
//				stmt.setBytes(1, chunk.getHash());
//				stmt.setBytes(2, chunk.getBytes());
//				int rowCount = stmt.executeUpdate();
//				System.out.println("DataChunk written to DB (" + rowCount + " rows inserted)");
//				
//				//now read the new entry using the chunk hash
//				sql = "select DataChunkId from FileChunk where Hash = ?";
//				stmt = connection.prepareStatement(sql);
//				stmt.setBytes(1, chunk.getHash());
//				ResultSet rs = stmt.executeQuery();
//				rs.next();
//				selectedDataChunkId = rs.getInt("DataChunkId");
//				System.out.println("selectedDataChunkId: " + selectedDataChunkId);	
//				
//				chunk.setFileChunkId(selectedDataChunkId);
//				
//				//now create a pointer to the chunk
//				System.out.println("creating FileVersionPointer...");
////				System.out.println("fileVersionId: " + fileVersion.getFileVersionId());
////				System.out.println("sequenceId: " + sequenceId);
////				System.out.println("FileChunkId: " + chunk.getFileChunkId());
//				sql = "insert into FileVersionPointers values (?, ?, ?)";
//				stmt = connection.prepareStatement(sql);
//				stmt.setInt(1, fileVersion.getFileVersionId());
//				stmt.setInt(2, sequenceId);
//				stmt.setInt(3, chunk.getFileChunkId());	
//				stmt.executeUpdate();
//				
//			} catch (SQLException e2){
//				System.err.println(e2);
//				return -1;
//			}
//			
//		}
//		
//
//		
//		
//		
//		return 1;
//		
////		System.out.println("done");
//	} //eom: storeFileChunk
	

	
}
