/**
 * 
 */
package message;

import java.net.InetAddress;

/**
 * @author remonx
 *
 */
public class DownloadFileMsg extends Message{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7833509890771396091L;
	
	private String fileFullPath;
	private boolean isSuccessful;
	
	public DownloadFileMsg(InetAddress desIP, int desPort) {
		this.desIP = desIP;
		this.desPort = desPort;
	}

	public String getFileFullPath() {
		return fileFullPath;
	}

	public void setFileFullPath(String fileFullPath) {
		this.fileFullPath = fileFullPath;
	}

	public boolean isSuccessful() {
		return isSuccessful;
	}

	public void setSuccessful(boolean isSuccessful) {
		this.isSuccessful = isSuccessful;
	}
	
	

}
