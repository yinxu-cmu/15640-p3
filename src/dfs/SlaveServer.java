package dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import exception.YZFSSlaveServiceException;

public class SlaveServer {
	public void startService(String masterHostName) throws YZFSSlaveServiceException,
			UnknownHostException, IOException, ClassNotFoundException {
		System.out.println("Slave Service Started");

		/* get connection to master server */
		System.out.println(masterHostName);
		Socket socket = new Socket(InetAddress.getByName(masterHostName),
				YZFS.MASTER_PORT);
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));

	}
}
