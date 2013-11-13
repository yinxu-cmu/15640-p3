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
import java.util.concurrent.atomic.AtomicInteger;

import mapreduce.MapReduceTask;
import message.*;

public class MasterServer extends Thread {
	private ServerSocket socketListener = null;

	public MasterServer() {
		try {
			socketListener = new ServerSocket(YZFS.MASTER_PORT);
		} catch (IOException e) {
			System.err.println("Fail to open socket during master server init.");
		}
	}

	/*
	 * keep accept connections from slave servers by listening to the listening
	 * socket
	 */
	public void run() {

		System.out.println("Master server started");
		
		System.out.println("Starting MapReduce master");
		MapReduceThread mpThread = new MapReduceThread();
		mpThread.start();

		while (true) {
			try {
				Socket socketServing = socketListener.accept();
				System.out.println("Socket accepted from " + socketServing.getInetAddress() + " "
						+ socketServing.getPort());

				/* read a message from the other end */
				ObjectInputStream input = new ObjectInputStream(socketServing.getInputStream());
				Message msg = (Message) input.readObject();

				/* if the incoming msg is from a slave server, 
				 * save all the information into the list for future use */
				if (msg.isFromSlave()) {
					SlaveInfo slaveInfo = new SlaveInfo();
					slaveInfo.iaddr = socketServing.getInetAddress();
					slaveInfo.port = socketServing.getPort();
					slaveInfo.input = socketServing.getInputStream();
					slaveInfo.output = socketServing.getOutputStream();
					slaveList.add(slaveInfo);
					System.out.println("One slave added");

				}

				/* send the reply msg after doing all the executions */
				Message reply = parseMessage(msg);
				ObjectOutputStream output = new ObjectOutputStream(socketServing.getOutputStream());
				output.writeObject(reply);
				output.flush();
				
				System.out.println("reply msg sent from master");

			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Fail to accept slave server request.");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * dispatch the message to particular method and return the reply msg
	 * @param msg
	 * @return
	 */
	private Message parseMessage(Message msg) throws IOException, ClassNotFoundException {
		if (msg instanceof CopyFromLocalMsg) {
			System.out.println("master server receive a copy form local message");
			executeCopyFromLocal((CopyFromLocalMsg) msg);
			return new AckMsg(true);
		} else if (msg instanceof ListMsg) {
			System.out.println("master server receive a list message");
			executeList((ListMsg) msg);
			return msg;
		} else if (msg instanceof RemoveMsg) {
			System.out.println("master server receive a remove message");
			executeRemove((RemoveMsg) msg);
			return new AckMsg(true);
		} else if (msg instanceof CatenateMsg) {
			System.out.println("master server receive a cat message");
			executeCatenate((CatenateMsg) msg);
			return msg;
		} else if (msg instanceof RequestFileMapMsg) {
			System.out.println("master server receive a RequestFileMap message");
			executeRequestFileMap((RequestFileMapMsg) msg);
			return msg;
		} else if (msg instanceof DownloadFileMsg) {
			System.out.println("master server receive a downloadfile message");
			executeDownloadFileMsg((DownloadFileMsg) msg);
			return msg;
		}
		return null;
	}

	/**
	 * get cat message from each part of the file and glue them together
	 * @param msg
	 */
	private void executeCatenate(CatenateMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		ArrayList<String> filePartList = fileToPart.get(msg.getFileName());
		StringBuilder strReply = new StringBuilder();
		
		/* get cat message from each part of the file and glue them together */
		for (String filePartName : filePartList) {
			SlaveInfo slaveInfo = partToSlave.get(filePartName).get(0);
			msg.setFilePartName(filePartName);
			Message reply = CommunicationModule.sendMessage(slaveInfo.input, slaveInfo.output, msg);
			strReply.append(((CatenateMsg) reply).getCatReply());
		}
		msg.setCatReply(strReply.toString());
	}

	/**
	 * remove the specific file one each slave server and update the fileList & partList
	 * @param msg
	 */
	private void executeRemove(RemoveMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		String fileName = msg.getFileName();
		ArrayList<String> partList = fileToPart.get(fileName);
		
		/* remove each file part on every slave server */
		for (String filePartName : partList) {
			msg.setFilePartName(filePartName);
			ArrayList<SlaveInfo> slaveList = partToSlave.get(filePartName);
			for (SlaveInfo slaveInfo : slaveList) {
				CommunicationModule.sendMessage(slaveInfo.input,
						slaveInfo.output, msg);
			}
			
			/* update the data structure */
			partToSlave.remove(filePartName);
		}
		
		/* update the data structure */
		fileToPart.remove(fileName);
	}

	/**
	 * list all files on the YZFS, store the reply String in the original msg
	 * @param msg
	 */
	private void executeList(ListMsg msg) {
		StringBuilder strReply = new StringBuilder();
		for (String str : fileToPart.keySet())
			strReply.append(str + ' '); //11.10 changed from \t to space
		strReply.insert(0, "Found " + fileToPart.size() + " items\n");
		msg.setListReply(strReply.toString());
	}

	/**
	 * tell random slaves to fetch file(s) or part of the file(s) from client side
	 * @param msg
	 */
	private void executeCopyFromLocal(CopyFromLocalMsg msg) throws IOException,
			ClassNotFoundException {
		
		/* send copy from local msg for every file in the file list */
		ArrayList<String> fileList = msg.getLocalFileListFullPath();
		ArrayList<Long> fileSize = msg.getLocalFileSize();
		int length = fileSize.size();
//		for (File file : fileList) {
		for (int i = 0; i < length; i++) {
			FilePartition filePartition = new FilePartition(fileList.get(i), fileSize.get(i));
			ArrayList<FileChunk> chunkList = filePartition.generateFileChunks();
			ArrayList<String> partList = new ArrayList<String>();
			
			/* send copy from local msg for every part of the file */
			for (FileChunk chunk : chunkList) {
				ArrayList<SlaveInfo> randomSlaveList = getRandomSlaves();
				msg.setFileChunk(chunk);
				Message ack = new Message();
				for (SlaveInfo slaveInfo : randomSlaveList) {
					ack = CommunicationModule.sendMessage(slaveInfo.input, slaveInfo.output, msg);
					if (ack instanceof AckMsg)
						System.out.println("ack from slave server");
				}
				/* add to partToSlave list */
				int partNum = chunk.partNum;
				String filePartName = msg.getFileName(fileList.get(i)) + ".part" + partNum;
				partToSlave.put(filePartName, randomSlaveList);
				partList.add(filePartName);
				
				System.out.println("send message to " + randomSlaveList.size() + " hosts");
			}
			/* add to fileToPart list */
			fileToPart.put(msg.getFileName(fileList.get(i)), partList);
			
		}

	}
	
	public void executeRequestFileMap(RequestFileMapMsg msg) {
		msg.setFileToPart(fileToPart);
		msg.setPartToSlave(partToSlave);
	}
	
	public void executeDownloadFileMsg(DownloadFileMsg msg) throws IOException {
		// ??? can use thread here to improve performace
		System.out.println("Start File Download from " + msg.getDesIP() + " "
				+ msg.getDesPort());
		Socket socket = new Socket(msg.getDesIP(), msg.getDesPort());

		InputStream input = socket.getInputStream();
		
		/* create the file and write what the server get from socket into the file */
		System.out.println(msg.getFileFullPath());
		
		FileOutputStream fileOutput = new FileOutputStream(msg.getFileFullPath());
		byte[] buffer = new byte[1024];
		int length = -1;
		while ((length = input.read(buffer)) > 0) {
			fileOutput.write(buffer, 0, length);
			fileOutput.flush();
		}

		System.out.println("Finish File Downlaod");
		msg.setSuccessful(true);
		socket.close();
		input.close();
		fileOutput.close();
	}

	/**
	 * get random slaves from slave list, 
	 * return all slaves if replication factor is greater than the number of slaves
	 * @return
	 */
	private ArrayList<SlaveInfo> getRandomSlaves() {
		ArrayList<SlaveInfo> ret = new ArrayList<SlaveInfo>(slaveList);
		if (YZFS.replicationFactor < slaveList.size()) {
			Collections.shuffle(ret);
			return new ArrayList<SlaveInfo>(ret.subList(0, (YZFS.replicationFactor)));
		} else {
			return ret;
		}
	}

	private HashSet<SlaveInfo> slaveList = new HashSet<SlaveInfo>();
	public static HashMap<String, ArrayList<String>> fileToPart = new HashMap<String, ArrayList<String>>();
	public static HashMap<String, ArrayList<SlaveInfo>> partToSlave = new HashMap<String, ArrayList<SlaveInfo>>();
	
	////
	private static boolean ongoing = true;
	public static Queue<MapReduceTask> mapQueue = new LinkedList<MapReduceTask>();
	public static Queue<MapReduceTask> reduceQueue = new LinkedList<MapReduceTask>();
	public static AtomicInteger jobId = new AtomicInteger(0);
	public static HashMap<Integer, Integer> jobToTaskCount = new HashMap<Integer, Integer>();
	////
}
