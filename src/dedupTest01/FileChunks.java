package dedupTest01;

import java.util.ArrayList;
import java.io.File;

public class FileChunks {
	
	ArrayList<FileChunk> mFileChunkArrayList = null;
	
	public FileChunks(){
		mFileChunkArrayList = new ArrayList<FileChunk>();
	}
	
	public static FileChunks createFileChunksFromFile(File file){
		
		
		
		return new FileChunks();
	}
	
	public void writeToDb(){
		
		
		
		
	}
	
	public void addFileChunk(FileChunk fileChunk){
		mFileChunkArrayList.add(fileChunk);
	}
	
	public FileChunk getFileChunk(int index){
		return mFileChunkArrayList.get(index);
	}
	
}
