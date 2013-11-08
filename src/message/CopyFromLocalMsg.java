package message;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;

import dfs.FileChunk;

public class CopyFromLocalMsg extends Message {

	public CopyFromLocalMsg(ArrayList<File> localFileList, InetAddress IP, int port) {
		this.localFileList = localFileList;
		this.fileTransferIP = IP;
		this.fileTransferPort = port;
	}
	
	public InetAddress getFileTransferIP() {
		return this.fileTransferIP;
	}

	public int getFileTransferPort() {
		return this.fileTransferPort;
	}
	
	public ArrayList<File> getLocalFileListFullPath() {
		return this.localFileList;
	}
	
	public String getFileName(String fileFullPath) {
		File file = new File(fileFullPath);
		return file.getName();
	}
	
	
	public FileChunk getFileChunk() {
		return fileChunk;
	}

	public void setFileChunk(FileChunk fileChunk) {
		this.fileChunk = fileChunk;
	}

	public long getFileLength() {
		return fileLength;
	}

	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}


	private static final long serialVersionUID = -5861057913250168882L;
	private ArrayList<File> localFileList;
	private InetAddress fileTransferIP;
	private int fileTransferPort;
	
	private FileChunk fileChunk;
	private long fileLength;
	
}
