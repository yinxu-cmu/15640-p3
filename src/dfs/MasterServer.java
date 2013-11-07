package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

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

		while (true) {
			try {
				Socket socketServing = socketListener.accept();
				System.out.println("Socket accepted from " + socketServing.getInetAddress() + " "
						+ socketServing.getPort());

				Message msg = null;
				ObjectInputStream input = new ObjectInputStream(socketServing.getInputStream());
				msg = (Message) input.readObject();

				if (msg.isFromSlave()) {
					/* save all the information into the list for future use */
					SlaveInfo slaveInfo = new SlaveInfo();
					slaveInfo.input = socketServing.getInputStream();
					slaveInfo.output = socketServing.getOutputStream();
					slaveList.add(slaveInfo);
					System.out.println("One slave added");

				}

				Message reply = this.parseMessage(msg);
				ObjectOutputStream output = new ObjectOutputStream(socketServing.getOutputStream());
				output.writeObject(reply);
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

	private Message parseMessage(Message msg) throws IOException, ClassNotFoundException {
		if (msg instanceof CopyFromLocalCommandMsg) {
			System.out.println("master server receive a copy form local message");
			executeCopyFromLocal((CopyFromLocalCommandMsg) msg);
			return new AckMsg(true);
		} else if (msg instanceof ListMsg) {
			System.out.println("master server receive a list message");
			executeList((ListMsg) msg);
			return msg;
		} else if (msg instanceof RemoveMsg) {
			System.out.println("master server receive a remove message");
			executeRemove((RemoveMsg) msg);
		} else if (msg instanceof CatenateMsg) {
			System.out.println("master server receive a cat message");
			executeCatenate((CatenateMsg) msg);
			return msg;
		}
		return null;
	}

	private void executeCatenate(CatenateMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		String fileName = msg.getFileName();
		SlaveInfo slaveInfo = this.masterFileList.get(fileName).get(0);
		Message reply = CommunicationModule.sendMessage(slaveInfo.input, slaveInfo.output, msg);
		msg.setCatReply(((CatenateMsg) reply).getCatReply());
	}

	private void executeRemove(RemoveMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		String fileName = msg.getFileName();
		ArrayList<SlaveInfo> slaveList = this.masterFileList.get(fileName);
		for (SlaveInfo slaveInfo : slaveList) {
			CommunicationModule.sendMessage(slaveInfo.input, slaveInfo.output, msg);
		}
		this.masterFileList.remove(fileName);
	}

	private void executeList(ListMsg msg) {
		StringBuilder strReply = new StringBuilder();
		for (String str : this.masterFileList.keySet())
			strReply.append(str + '\t');
		strReply.insert(0, "Found " + this.masterFileList.size() + " items\n");
		msg.setListReply(strReply.toString());
	}

	private void executeCopyFromLocal(CopyFromLocalCommandMsg msg) throws IOException,
			ClassNotFoundException {
		ArrayList<SlaveInfo> randomSlaveList = this.getRandomSlaves();
		Message ack = new Message();
		for (SlaveInfo slaveInfo : randomSlaveList) {
			ack = CommunicationModule.sendMessage(slaveInfo.input, slaveInfo.output, msg);
			if (ack instanceof AckMsg)
				System.out.println("ack from slave server");
			slaveInfo.fileList.add(msg.getFileName());
		}
		this.masterFileList.put(msg.getFileName(), randomSlaveList);
		System.out.println("send message to " + randomSlaveList.size() + " hosts");

	}

	private ArrayList<SlaveInfo> getRandomSlaves() {
		ArrayList<SlaveInfo> ret = new ArrayList<SlaveInfo>(this.slaveList);
		if (YZFS.replicationFactor < this.slaveList.size()) {
			Collections.shuffle(ret);
			return new ArrayList<SlaveInfo>(ret.subList(0, (YZFS.replicationFactor)));
		} else {
			return ret;
		}
	}

	private String catCommand(String file) {
		BufferedReader buffer = null;
		StringBuilder ret = new StringBuilder();
		try {
			buffer = new BufferedReader(new FileReader(this.directoryPath + file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			ret.append("No such file\n");
		}

		String currentLine = null;
		try {
			while ((currentLine = buffer.readLine()) != null) {
				ret.append(currentLine + '\n');
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.print(ret);
		return ret.toString();
	}

	private HashSet<SlaveInfo> slaveList = new HashSet<SlaveInfo>();
	private HashMap<String, ArrayList<SlaveInfo>> masterFileList = new HashMap<String, ArrayList<SlaveInfo>>();
	private final String directoryPath = "/tmp/YZFS/";
}
