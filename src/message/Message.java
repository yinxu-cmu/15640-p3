package message;

import java.io.Serializable;
import java.net.InetAddress;

public class Message implements Serializable {

	private static final long serialVersionUID = -1870514907425528873L;
	protected InetAddress desIP;
	protected InetAddress srcIP;
	protected int desPort;
	protected int srcPort;
	private boolean isFromSlave = false;
	
	public void setDes(InetAddress desIP, int desPort) {
		this.desIP = desIP;
		this.desPort = desPort;
	}
	
	public void setSrc(InetAddress srcIP, int srcPort) {
		this.srcIP = srcIP;
		this.srcPort = srcPort;
	}

	public InetAddress getDesIP() {
		return desIP;
	}

	public void setDesIP(InetAddress desIP) {
		this.desIP = desIP;
	}

	public int getDesPort() {
		return desPort;
	}

	public void setDesPort(int desPort) {
		this.desPort = desPort;
	}

	public InetAddress getSrcIP() {
		return srcIP;
	}

	public void setSrcIP(InetAddress srcIP) {
		this.srcIP = srcIP;
	}

	public int getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(int srcPort) {
		this.srcPort = srcPort;
	}

	public boolean isFromSlave() {
		return isFromSlave;
	}

	public void setFromSlave(boolean isFromSlave) {
		this.isFromSlave = isFromSlave;
	}

}
