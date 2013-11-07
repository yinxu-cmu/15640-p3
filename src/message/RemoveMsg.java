package message;

import java.io.File;

public class RemoveMsg extends Message{

	public RemoveMsg(String localFileFullPath) {
		this.localFileFullPath = localFileFullPath;
	}
	
	public String getFileName() {
		File file = new File(this.localFileFullPath);
		return file.getName();
	}
	
	private static final long serialVersionUID = -226484318375906067L;
	private String localFileFullPath = null;
}
