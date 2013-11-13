package dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import mapreduce.MapReduceTask;
import message.*;

public class MasterServer {
//	private ServerSocket socketListener = null;

	public MasterServer() {
		try {
			ServerSocket socketListener = new ServerSocket(YZFS.MASTER_PORT);
			System.out.println("Master server started");

			while (true) {
				Socket socketServing = socketListener.accept();
				System.out.println("Socket accepted from " + socketServing.getInetAddress() + " "
						+ socketServing.getPort());
				MasterServerThread masterThread = new MasterServerThread(socketServing);
				masterThread.start();
			}

		} catch (IOException e) {
			System.err.println("Fail to open socket during master server init.");
		}
	}

	// public void run() {
	//
	// //
	// // MapReduceThread mpThread = new MapReduceThread();
	// // mpThread.start();
	//
	// while (true) {
	// try {
	// Socket socketServing = socketListener.accept();
	// System.out.println("Socket accepted from " +
	// socketServing.getInetAddress() + " "
	// + socketServing.getPort());
	//
	// /* read a message from the other end */
	// ObjectInputStream input = new
	// ObjectInputStream(socketServing.getInputStream());
	// Message msg = (Message) input.readObject();
	//
	// /* if the incoming msg is from a slave server,
	// * save all the information into the list for future use */
	// if (msg.isFromSlave()) {
	// SlaveInfo slaveInfo = new SlaveInfo();
	// slaveInfo.iaddr = socketServing.getInetAddress();
	// slaveInfo.port = socketServing.getPort();
	// slaveInfo.input = socketServing.getInputStream();
	// slaveInfo.output = socketServing.getOutputStream();
	// slaveList.add(slaveInfo);
	// System.out.println("One slave added");
	//
	// }
	//
	// /* send the reply msg after doing all the executions */
	// Message reply = parseMessage(msg);
	// ObjectOutputStream output = new
	// ObjectOutputStream(socketServing.getOutputStream());
	// output.writeObject(reply);
	// output.flush();
	//
	// System.out.println("reply msg sent from master");
	//
	// } catch (IOException e) {
	// e.printStackTrace();
	// System.err.println("Fail to accept slave server request.");
	// } catch (ClassNotFoundException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	// }

	private Socket servingSocket = null;
	public static Set<SlaveInfo> slaveList = Collections
			.newSetFromMap(new ConcurrentHashMap<SlaveInfo, Boolean>());
	public static ConcurrentHashMap<String, ArrayList<String>> fileToPart = new ConcurrentHashMap<String, ArrayList<String>>();
	public static ConcurrentHashMap<String, ArrayList<SlaveInfo>> partToSlave = new ConcurrentHashMap<String, ArrayList<SlaveInfo>>();

	// //
	private static boolean ongoing = true;
	public static Queue<MapReduceTask> mapQueue = new LinkedList<MapReduceTask>();
	public static Queue<MapReduceTask> reduceQueue = new LinkedList<MapReduceTask>();
	public static AtomicInteger jobId = new AtomicInteger(0);
	public static HashMap<Integer, Integer> jobToTaskCount = new HashMap<Integer, Integer>();
	// //
}
