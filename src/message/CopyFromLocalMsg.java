package message;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;

public class CopyFromLocalMsg extends Message {

	public CopyFromLocalMsg(ArrayList<String> localFileListFullPath, InetAddress IP, int port) {
		this.localFileListFullPath = localFileListFullPath;
		this.fileTransferIP = IP;
		this.fileTransferPort = port;
	}
	
	public InetAddress getFileTransferIP() {
		return this.fileTransferIP;
	}

	public int getFileTransferPort() {
		return this.fileTransferPort;
	}
	
	public ArrayList<String> getLocalFileListFullPath() {
		return this.localFileListFullPath;
	}
	
	public void setLocalFileFullPath(String localFileFullPath) {
		this.localFileFullPath = localFileFullPath;
	}
	
	public String getLocalFileFullPath() {
		return this.localFileFullPath;
	}
	
	public String getFileName(String fileFullPath) {
		File file = new File(fileFullPath);
		return file.getName();
	}
	
	private static final long serialVersionUID = -5861057913250168882L;
	private ArrayList<String> localFileListFullPath;
	private String localFileFullPath;
	private InetAddress fileTransferIP;
	private int fileTransferPort;
	
	
}
