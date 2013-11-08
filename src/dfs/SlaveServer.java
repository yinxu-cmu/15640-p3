package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import message.*;

import exception.YZFSSlaveServiceException;

/**
 * This is a background running file system slave service
 * 
 * @author zhengk
 */
public class SlaveServer {
	public void startService(String masterHostName) throws YZFSSlaveServiceException,
			UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Slave Service Started");
		this.createWorkingDirectory();

		/* get connection to master server */
		System.out.println(masterHostName);
		Socket socket = new Socket(InetAddress.getByName(masterHostName), YZFS.MASTER_PORT);

		/*
		 * after successfully connect to master server, right master hostname
		 * and port number into a file, so that file system command line process
		 * can utilize it
		 */
		this.writeMasterInfo(masterHostName, YZFS.MASTER_PORT);
		Message msg = new Message();
		msg.setFromSlave(true);
		OutputStream output = socket.getOutputStream();
		ObjectOutputStream objOutput = new ObjectOutputStream(output);
		objOutput.writeObject(msg);
		objOutput.flush();

		InputStream input = socket.getInputStream();
		ObjectInputStream objInput = null;
		while (true) {
			objInput = new ObjectInputStream(input);
			msg = (Message) objInput.readObject();
			if (msg instanceof CopyFromLocalMsg) {
				System.out.println("slave server receive a copy from local message");
				executeCopyFromLocal((CopyFromLocalMsg) msg);
				AckMsg ack = new AckMsg(true);
				objOutput = new ObjectOutputStream(output);
				objOutput.writeObject(ack);
				objOutput.flush();
			} else if (msg instanceof RemoveMsg) {
				System.out.println("slave server receive a remove message");
				executeRemove((RemoveMsg) msg);
				AckMsg ack = new AckMsg(true);
				objOutput = new ObjectOutputStream(output);
				objOutput.writeObject(ack);
				objOutput.flush();
			} else if (msg instanceof CatenateMsg) {
				System.out.println("slave server receive a catenate message");
				executeCatenate((CatenateMsg) msg);
				objOutput = new ObjectOutputStream(output);
				objOutput.writeObject(msg);
				objOutput.flush();
			}
		}

	}
	
	private void executeCatenate (CatenateMsg msg) {
		BufferedReader buffer = null;
		StringBuilder ret = new StringBuilder();
		try {
			buffer = new BufferedReader(new FileReader(YZFS.fileSystemWorkingDir + msg.getFilePartName()));
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
		msg.setCatReply(ret.toString());
	}
	
	private void executeRemove(RemoveMsg msg) {
		File file = new File(YZFS.fileSystemWorkingDir + msg.getFilePartName());
		file.delete();
	}

	private void writeMasterInfo(String masterHostName, int masterPort)
			throws FileNotFoundException, IOException {
		Properties prop = new Properties();

		// set the properties value
		prop.setProperty("master host name", masterHostName);
		prop.setProperty("master port number", "" + masterPort);

		// save properties to project root folder
		prop.store(new FileOutputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"), null);

	}

	private void createWorkingDirectory() {
		File folder = new File(YZFS.fileSystemWorkingDir);
		/* create working directory */
		if (!folder.exists()) {
			if (folder.mkdir()) {
				System.out.println("Working Directory is created!");
			} else {
				System.err.println("Failed to create directory!");
			}
		}
		/* delete all files in the directory */
		else {
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles)
				file.delete();
		}
	}

	private boolean executeCopyFromLocal(CopyFromLocalMsg msg) throws IOException {
		System.out.println("Start File Transfer from " + msg.getFileTransferIP() + " "
				+ msg.getFileTransferPort());
		Socket socket = new Socket(msg.getFileTransferIP(), msg.getFileTransferPort());

		// send the file name and range
		OutputStream output = socket.getOutputStream();
		ObjectOutputStream objOutput = new ObjectOutputStream(output);
		objOutput.writeObject(msg.getFileChunk());
		objOutput.flush();
		
		InputStream input = socket.getInputStream();
		
		FileOutputStream fileOutput = new FileOutputStream(YZFS.fileSystemWorkingDir
				+ msg.getFileName(msg.getFileChunk().localFileFullPath) + ".part" + msg.getFileChunk().partNum);
		byte[] buffer = new byte[YZFS.RECORD_LENGTH];
		int length = -1;
		while ((length = input.read(buffer)) > 0) {
			fileOutput.write(buffer, 0, length);
			fileOutput.flush();
		}

		System.out.println("Finish File Transfer");
		return true;
	}
}
