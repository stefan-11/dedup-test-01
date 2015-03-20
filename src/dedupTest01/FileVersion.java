package dedupTest01;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FileVersion {

	private int mFileVersionId = 0; //0 means that no FileVersionId was set up to now
	private String mPath = null;
	private String mFilename = null;
	private int mVersion = 0; //0 means that no Version was set yet (maybe a version already exists in DB).
	private String mFileVersionHash = null;
	private boolean mExistsInDB = false;
	
	/**
	 * Constructor to create a FileVersion instance from a java File
	 * 
	 * @param file
	 */
	public FileVersion(File file) throws SQLException{
		String fileAndName = file.getAbsolutePath();
		mPath = fileAndName.substring(0, fileAndName.lastIndexOf(File.separator)) + File.separator;
		mFilename = fileAndName.substring(fileAndName.lastIndexOf(File.separator)+1);
		
		//create a hash of the file
		String newHash = new FileHash(file).toString();
		System.out.println("newHash: " + newHash);
		mFileVersionHash = newHash;
		
		
		Connection connection = DBAccess.getInstance().getConnection();
		
		//check if a FileVersion with this hash value already exists
		String sql = "select FileVersionId from FileVersion where FileVersionHash = ?";
		PreparedStatement stmt = connection.prepareStatement(sql);	
		stmt.setString(1, mFileVersionHash);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		try {
			mFileVersionId = rs.getInt("FileVersionId");
		} catch (SQLException e){
			mFileVersionId = 0;
		}
		
		System.out.println("mFileVersionId: " + mFileVersionId);
		
		if (mFileVersionId == 0){
			//no existing DataChunk found with the same hash...so create a new one
			System.out.println("create new FileVersion");
		
			//bytes are written to DB immediatedly
			sql = "insert into FileVersion values (NULL, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)";
			stmt = connection.prepareStatement(sql);
			stmt.setString(1, mPath);
			stmt.setString(2, mFilename);
			stmt.setString(3, "1");
			stmt.setString(4, "CREATING");
			stmt.setString(5, newHash);
			int rowCount = stmt.executeUpdate();
			System.out.println("FileVersion written to DB (" + rowCount + " rows inserted)");
			
			//get the just created DataChunkId
			sql = "select FileVersionId from FileVersion where OID = last_insert_rowid();";
			stmt = connection.prepareStatement(sql);	
			rs = stmt.executeQuery();
			rs.next();
			mFileVersionId = rs.getInt("FileVersionId");
		
		} else {
			//a DataChunk with this hash was already found...do nothing
//			System.out.println("existing DataChunk with same hash found. No new DataChunk created");
		}
	}
	
	/**
	 * Constructor to create a FileVersion instance by a given FileVersionId
	 * @param FileVersionId
	 */
	public FileVersion(int FileVersionId){
		System.out.println("Constructor FileVersion(int FileVersionId) is not implemented yet");
	}
	

	
	
	public int getFileVersionId(){
		return mFileVersionId;
	}
	
	public void setFileVersionId(int fileVersionId){
		mFileVersionId = fileVersionId;
	}
	
	public String getPath(){
		return mPath;
	}
	
	public String getFilename(){
		return mFilename;
	}
	
	public void setFilename(String filename){
		mFilename = filename;
	}
	
	public int getVersion(){
		return mVersion;
	}
	
	public void setVersion(int newVersion){
		mVersion = newVersion;
	}
	
	public String getFileVersionHash(){
		return mFileVersionHash;
	}
	
	public void setFileVersionHash(String fileVersionHash){
		mFileVersionHash = fileVersionHash;
	}
	
	public boolean existsInDB(){
		return mExistsInDB;
	}
	
	public void setExistsInDB(boolean flag){
		mExistsInDB = flag;
	}
	
	
	
	public void writeToFile(){
		System.out.println("writing FileVersion to file " + mPath + mFilename);
		
		String filenameAndPath = mPath + mFilename;
		
		FileOutputStream fop = null;
		File file;
//		String content = "This is the text content";
 
		try {
 
			file = new File(filenameAndPath);
			fop = new FileOutputStream(file);
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			
			String sql = "select * from FileVersionsPointers where FileVersionId = ?";
//			PreparedStatement stmt
			
			// get the content in bytes
//			byte[] contentInBytes = content.getBytes();
// 
//			fop.write(contentInBytes);
//			fop.flush();
//			fop.close();
 
			System.out.println("Done");
 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
