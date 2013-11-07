package message;

import java.io.File;

public class CatenateMsg extends Message{
	
	public CatenateMsg(String localFileFullPath) {
		File file = new File(localFileFullPath);
		this.fileName = file.getName();
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	public String getCatReply() {
		return catReply;
	}

	public void setCatReply(String catReply) {
		this.catReply = catReply;
	}
	
	private String fileName = null;
	private String catReply = null;
	private static final long serialVersionUID = -722239763841540007L;

}
