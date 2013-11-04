package message;

import java.io.File;
import java.net.InetAddress;

public class CopyFromLocalCommandMsg extends Message {

	public CopyFromLocalCommandMsg(String localFileLocation, InetAddress IP, int port) {
		this.localFileLocation = localFileLocation;
		this.fileTransferIP = IP;
		this.fileTransferPort = port;
	}
	
	public InetAddress getFileTransferIP() {
		return this.fileTransferIP;
	}

	public int getFileTransferPort() {
		return this.fileTransferPort;
	}
	
	public String getFileName() {
		File file = new File(this.localFileLocation);
		return file.getName();
	}
	
	private static final long serialVersionUID = -5861057913250168882L;
	private String localFileLocation;
	private InetAddress fileTransferIP;
	private int fileTransferPort;
	
	
}
