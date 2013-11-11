/**
 * 
 */
package message;

import java.util.ArrayList;
import java.util.HashMap;


import dfs.SlaveInfo;

/**
 * @author yinxu
 *
 */
public class RequestFileMapMsg extends Message{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2172446017665098577L;
	
	private HashMap<String, ArrayList<String>> fileToPart;
	private HashMap<String, ArrayList<SlaveInfo>> partToSlave;
	
//	public RequestFileMapMsg(HashMap<String, ArrayList<String>> fileToPart, 
//			HashMap<String, ArrayList<SlaveInfo>> partToSlave) {
//		this.fileToPart = fileToPart;
//		this.partToSlave = partToSlave;
//	}

	public HashMap<String, ArrayList<String>> getFileToPart() {
		return fileToPart;
	}

	public void setFileToPart(HashMap<String, ArrayList<String>> fileToPart) {
		this.fileToPart = fileToPart;
	}

	public HashMap<String, ArrayList<SlaveInfo>> getPartToSlave() {
		return partToSlave;
	}

	public void setPartToSlave(HashMap<String, ArrayList<SlaveInfo>> partToSlave) {
		this.partToSlave = partToSlave;
	}
	

}
