package dedupTest01;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;

public class dedupTest01 {
	
	public static void main(String[] args) throws Exception{
		
//		FileStorage.getInstance();
		
		dedupTest01 dedup = new dedupTest01();
		
		//create chunks and write them into sqlite DB
		dedup.createChunksFromFile("testfile.txt");
//		dedup.createChunksFromFile("smallfile.txt");
		
//		FileStorage.getInstance().closeDB();
		
		
//		File aFile = new File("smallfile.txt");
//		FileHash testHash = new FileHash(aFile);
////		byte[] hash = testHash.getBytes();
//		System.out.println("hash: " + testHash);

	}
	
	
	public void createChunksFromFile(String filename) {
		
		//open the file for reading
	    File aFile = new File(filename);
	    FileInputStream inFile = null;
	    
	    try {
	    	inFile = new FileInputStream(aFile);

		    FileChannel inChannel = inFile.getChannel();
		    ByteBuffer buf = ByteBuffer.allocate(1024*64);
		    
		    FileVersion fileVersion = new FileVersion(aFile);
		    System.out.println("new FileVersion created");
		    
		    int sequenceId = 0;
		    
		    while (inChannel.read(buf) != -1) {
//		    	System.out.println("reading sequenceId " + sequenceId);
		    	buf.flip();
		    	byte[] byteArray = buf.array();
//		    	String test = new String(byteArray);
//		    	System.out.println("test: " + test);
//		    	System.out.println("String read: " + ((ByteBuffer) (buf.flip())).asCharBuffer().get(0));
		    	
		    	DataChunk dataChunk = new DataChunk(byteArray);
		    	FileChunk fileChunk = new FileChunk(fileVersion.getFileVersionId(), sequenceId, dataChunk.getDataChunkId());
		    	
		    	buf.clear();
		    	
		    	sequenceId++;
		    }
		    inFile.close();
		    
		    System.out.println("sequenceId: " + sequenceId);
		    
	    } catch (FileNotFoundException e1){
	    	System.out.println("Error: File not found in FileStorage.storeFileChunksForFileVersions");
	    	e1.printStackTrace();
	    } catch (IOException e2){
	    	System.out.println("Error: Error reading file in FileStorage.storeFileChunksForFileVersions");
	    	e2.printStackTrace();
	    } catch (SQLException e3){
	    	System.out.println("Error: Error accessing DB");
	    	e3.printStackTrace();	    	
	    }
		
		
		
	}
	
	
	
	
	
	
}
