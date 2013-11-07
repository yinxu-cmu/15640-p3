package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import message.AckMsg;
import message.CopyFromLocalCommandMsg;
import message.Message;

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
		ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
		output.writeObject(msg);
		output.flush();

		InputStream input = socket.getInputStream();
		ObjectInputStream objInput = null;
		while (true) {
			objInput = new ObjectInputStream(input);
			msg = (Message) objInput.readObject();
			if (msg instanceof CopyFromLocalCommandMsg) {
				System.out.println("slave server receive a copy from local message");
				executeCopyFromLocal((CopyFromLocalCommandMsg) msg);
//				AckMsg ack = new AckMsg(true);
//				output.reset();
//				output.writeObject(ack);
//				output.flush();
			}
		}

	}

	public void writeMasterInfo(String masterHostName, int masterPort)
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

	private boolean executeCopyFromLocal(CopyFromLocalCommandMsg msg) throws IOException {
		System.out.println("Start File Transfer from " + msg.getFileTransferIP()
				+ msg.getFileTransferPort());
		Socket socket = new Socket(msg.getFileTransferIP(), msg.getFileTransferPort());
		InputStream input = socket.getInputStream();
		FileOutputStream output = new FileOutputStream(YZFS.fileSystemWorkingDir
				+ msg.getFileName());
		byte[] buffer = new byte[4096];
		int length = -1;
		while ((length = input.read(buffer)) > 0) {
			output.write(buffer, 0, length);
			output.flush();
		}

		System.out.println("Finish File Transfer");
		return true;
	}
}
