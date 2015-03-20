package dedupTest01;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;

public class DataChunk {

	private int mDataChunkId = 0;
	
	/**
	 * Create DataChunk with bytes
	 * 
	 * @param bytes
	 * @throws SQLException
	 */
	DataChunk(byte[] bytes) throws SQLException{
		//create a new DataChunk and write it to DB immediatedly
		
		Connection connection = DBAccess.getInstance().getConnection();
		
		//check if a dataChunk with this hash value already exists
		String sql = "select DataChunkId from DataChunk where Hash = ?";
		PreparedStatement stmt = connection.prepareStatement(sql);	
		stmt.setBytes(1, DataChunk.getHash(bytes));
		ResultSet rs = stmt.executeQuery();
		rs.next();
		try {
			mDataChunkId = rs.getInt("DataChunkId");
		} catch (SQLException e){
			mDataChunkId = 0;
		}
		
		System.out.println("mDataChunkId: " + mDataChunkId);
		
		if (mDataChunkId == 0){
			//no existing DataChunk found with the same hash...so create a new one
//			System.out.println("create new DataChunk");
		
			//bytes are written to DB immediatedly
			sql = "insert into DataChunk values (NULL, ?, ?)";
			stmt = connection.prepareStatement(sql);
			stmt.setBytes(1, DataChunk.getHash(bytes));
			stmt.setBytes(2, bytes);
			int rowCount = stmt.executeUpdate();
			System.out.println("DataChunk written to DB (" + rowCount + " rows inserted)");
			
			//get the just created DataChunkId
			sql = "select DataChunkId from DataChunk where OID = last_insert_rowid();";
			stmt = connection.prepareStatement(sql);	
			rs = stmt.executeQuery();
			rs.next();
			mDataChunkId = rs.getInt("DataChunkId");
		
		} else {
			//a DataChunk with this hash was already found...do nothing
//			System.out.println("existing DataChunk with same hash found. No new DataChunk created");
		}
		
	}
	
	
	public static byte[] getHash(byte[] byteArray){
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			digest.update(byteArray);
			
			return digest.digest();
		} catch (NoSuchAlgorithmException e){
			System.err.println("FileChunk.getHash failed");
			e.printStackTrace();
			return null;
		}

	}
	
	public String toString(){
		return new String("DataChunk id: " + mDataChunkId);
	}
	
	public byte[] getBytes(){
		byte[] byteArray = new byte[5];
		return byteArray;
	}
	
	public void setDataChunkId(int dataChunkId){
		mDataChunkId = dataChunkId;
	}
	
	public int getDataChunkId(){
		return mDataChunkId;
	}
}
