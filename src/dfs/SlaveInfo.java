package dfs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashSet;

public class SlaveInfo {

	public Socket socket;
	public InetAddress iaddr;
	public int port;
	public InputStream input;
	public OutputStream output;
	
	public HashSet<String> fileList = new HashSet<String>();
	
	/* master server's input or output stream */
//	public BufferedReader in;
//	public PrintWriter out;

	public String toString() {
		return "\tInetAddress: " + iaddr + "\tport number: " + port;
	}
}
