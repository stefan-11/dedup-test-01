package dedupTest01;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.io.IOException;

public class FileHash {
	
	int fileChunkSize = 1048576;
	
	File mFile = null;
	byte[] mHash = null;
	
	/**
	 * Constructor
	 * 
	 * @param file
	 */
	public FileHash(File file){
		mFile = file;
	}
	
	public byte[] getBytes(){
		
		ArrayList<byte[]> list0 = createHashListFromFile(mFile);
		
		
//		System.out.println("list0.size: " + list0.size());
		
		while (true){
			
			ArrayList<byte[]> list1 = new ArrayList<byte[]>();
			
			//create hashes for all elements
			int count = 0;
			while (count < list0.size()){
				//combine next two elements
				byte[] bytes1 = null;
				byte[] bytes2 = null;
				byte[] combinedBytes = null;
				try {
					//get next two byte arrays
					bytes1 = list0.get(count);
					count++;
					bytes2 = list0.get(count);
					count++;				
					combinedBytes = new byte[bytes1.length + bytes2.length];
					
					//now combine the two parts to one byte array
//					System.out.println("adding bytes1");
					for (int i=0; i<bytes1.length; i++){
						combinedBytes[i] = bytes1[i];
					}
//					System.out.println("adding bytes2");
					for (int i=0; i<bytes2.length; i++){
						combinedBytes[i+bytes1.length] = bytes2[i];
					}
					
					//create a hash of the combined byte array
//					DataChunk chunk = new DataChunk(combinedBytes);
					list1.add(DataChunk.getHash(combinedBytes));
				} catch (IndexOutOfBoundsException e){
					//this exception occurs if there is no second next byte array
					//in this case the one element is hashed alone
//					System.out.println("single last byte array...");
					combinedBytes = list0.get(list0.size()-1);
//					DataChunk chunk = new DataChunk(combinedBytes);
					list1.add(DataChunk.getHash(combinedBytes));				
				}
				
			}
			
			System.out.println("list1.size " + list1.size());
			
			list0 = list1;
			
			if (list1.size() == 1){
				break;
			}
			
			list1 = null;
		}
		
		mHash = list0.get(0);
		return mHash;
	}
	
	/**
	 * create hash list from the file
	 * 
	 * @param file
	 * @return
	 */
	private ArrayList<byte[]> createHashListFromFile(File file){
		
		try {
			ArrayList<byte[]> stage0List = new ArrayList();
			
		    FileInputStream inFile = null;
	
		    inFile = new FileInputStream(file);
		    
		    FileChannel inChannel = inFile.getChannel();
		    ByteBuffer buf = ByteBuffer.allocate(fileChunkSize);
		    
		    while (inChannel.position() < inChannel.size()){
		    	int byteCount = inChannel.read(buf);
//		    	System.out.println("byteCount: " + byteCount);
		    	
		    	buf.flip();
		    	byte[] byteArray = buf.array();
		    	
		    	//if read bytes do not fill buffer create a new smaller buffer
		    	if (byteCount < buf.array().length){
//		    		System.out.println("creating new smaller bytebuffer");
		    		byteArray = new byte[byteCount];
		    	}
		    	
		    	buf.clear();
		    	
		    	//make hashes immediatedly to avoid holding all data in memory
//		    	DataChunk chunk = new DataChunk(byteArray);
		    	stage0List.add(DataChunk.getHash(byteArray));
		    	
		    }
		    inFile.close();
		    
		    return stage0List;
		    
		} catch (IOException e){
			System.err.println("Error in FileHash.createHashListStage0");
			return null;
		}
	} //eom: createHashListStage0
	
	
	public String toString(){
		
		StringBuilder sb = null;
		
		if (mHash == null){
			mHash = this.getBytes();
		}
		
		sb = new StringBuilder();
	    for (byte b : mHash) {
	        sb.append(String.format("%02X", b));
	    }
//		System.out.println(sb.toString());			

	    return sb.toString();
	}
	
}
