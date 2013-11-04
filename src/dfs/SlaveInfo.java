package dfs;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class SlaveInfo {

	public Socket socket;
	public InetAddress iaddr;
	public int port;
	/* master server's input or output stream */
	public BufferedReader in;
	public PrintWriter out;

	public String toString() {
		return "\tInetAddress: " + iaddr + "\tport number: " + port;
	}
}
