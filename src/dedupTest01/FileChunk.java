package dedupTest01;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FileChunk {
	
	private int mFileVersionId = 0;
	private int mSequenceId = 0;
	private int mDataChunkId = 0;
	
	private boolean changedFlag = false; 
	
	public FileChunk(int fileVersionId, int sequenceId, int dataChunkId) throws SQLException{
		//create a FileChunk and write it to DB immediately
		
		Connection connection = DBAccess.getInstance().getConnection();
		
		//check if a dataChunk with this hash value already exists
		String sql = "select FileVersionId, SequenceId, DataChunkId from FileChunk where FileVersionId = ? AND SequenceId = ? AND DataChunkId = ?";
		PreparedStatement stmt = connection.prepareStatement(sql);	
		stmt.setInt(1, fileVersionId);
		stmt.setInt(2, sequenceId);
		stmt.setInt(3, dataChunkId);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		try {
			mFileVersionId = rs.getInt("FileVersionId");
			mSequenceId = rs.getInt("SequenceId");
			mDataChunkId = rs.getInt("DataChunkId");
		} catch (SQLException e){
			mFileVersionId = 0;
			mSequenceId = 0;
			mDataChunkId = 0;
		}
		
		System.out.println("mDataChunkId: " + mDataChunkId);
		
		if (mFileVersionId == 0 &&
			mSequenceId == 0 &&
			mDataChunkId == 0){
			//no existing DataChunk found with the same hash...so create a new one
			System.out.println("create new FileChunk");
		
			//bytes are written to DB immediately
			sql = "insert into FileChunk values (?, ?, ?)";
			stmt = connection.prepareStatement(sql);
			stmt.setInt(1, fileVersionId);
			stmt.setInt(2, sequenceId);
			stmt.setInt(3, dataChunkId);
			int rowCount = stmt.executeUpdate();
			System.out.println("FileChunk written to DB (" + rowCount + " rows inserted)");
		
		} else {
			//a DataChunk with this hash was already found...do nothing
			System.out.println("existing FileChunk with same hash found. No new FileChunk created");
		}
		
	}
	
	
	public String toString(){
		return new String("FileChunk: fileVersionId: " + mFileVersionId);
	}

	
}
