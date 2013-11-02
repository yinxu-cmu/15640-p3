package dfs;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.InetAddress;

public class SlaveInfo {
	
		public InetAddress iaddr;
		public int port;
		/* master server's input or output stream */
		public BufferedReader in;
		public PrintWriter out;
		
		public String toString() {
			return "\tInetAddress: " + iaddr +
					"\tport number: " + port;
		}
}
