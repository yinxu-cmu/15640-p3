/**
 * 
 */
package message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


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
	
	private ConcurrentHashMap<String, ArrayList<String>> fileToPart;
	private ConcurrentHashMap<String, ArrayList<SlaveInfo>> partToSlave;
	
//	public RequestFileMapMsg(HashMap<String, ArrayList<String>> fileToPart, 
//			HashMap<String, ArrayList<SlaveInfo>> partToSlave) {
//		this.fileToPart = fileToPart;
//		this.partToSlave = partToSlave;
//	}

	public ConcurrentHashMap<String, ArrayList<String>> getFileToPart() {
		return fileToPart;
	}

	public void setFileToPart(ConcurrentHashMap<String, ArrayList<String>> fileToPart2) {
		this.fileToPart = fileToPart2;
	}

	public ConcurrentHashMap<String, ArrayList<SlaveInfo>> getPartToSlave() {
		return partToSlave;
	}

	public void setPartToSlave(ConcurrentHashMap<String, ArrayList<SlaveInfo>> partToSlave2) {
		this.partToSlave = partToSlave2;
	}
	

}
