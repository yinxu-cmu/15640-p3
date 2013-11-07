package dfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import message.*;

public class CommandLine {

	public CommandLine() throws FileNotFoundException, IOException {
		// load a properties file and read master ip and port
		Properties prop = new Properties();
		prop.load(new FileInputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"));
		this.masterIP = InetAddress.getByName(prop.getProperty("master host name"));
		this.masterPort = Integer.parseInt(prop.getProperty("master port number"));
	}

	public void parseCommandLine(String[] args) throws FileNotFoundException, IOException,
			ClassNotFoundException {
		/* args[0] always equals to "-yzfs" */
		if (args[1].equals("copyFromLocal"))
			copyFromLocal(args[2]);
		else if (args[1].equals("ls"))
			list();
		else if (args[1].equals("rm"))
			remove(args[2]);
		else if (args[1].equals("cat"))
			catenate(args[2]);
	}

	private void catenate(String localFileFullPath) throws UnknownHostException, IOException,
			ClassNotFoundException {
		System.out.println("catenate command line parsed");
		CatenateMsg request = new CatenateMsg(localFileFullPath);
		request.setDes(masterIP, masterPort);
		Message reply = CommunicationModule.sendMessage(request);
		System.out.println(((CatenateMsg) reply).getCatReply());
	}

	private void remove(String localFileFullPath) throws UnknownHostException, IOException,
			ClassNotFoundException {
		System.out.println("remove command line parsed");
		RemoveMsg request = new RemoveMsg(localFileFullPath);
		request.setDes(masterIP, masterPort);
		CommunicationModule.sendMessage(request);
	}

	private void list() throws IOException, ClassNotFoundException {
		System.out.println("list command line parsed modified");
		ListMsg request = new ListMsg();
		request.setDes(masterIP, masterPort);
		Message reply = CommunicationModule.sendMessage(request);
		System.out.println(((ListMsg) reply).getListReply());
	}

	private void copyFromLocal(String localFileFullPath) throws FileNotFoundException, IOException,
			ClassNotFoundException {
		System.out.println("copy from local command line parsed");

		CopyFromLocalCommandMsg request = new CopyFromLocalCommandMsg(localFileFullPath,
				InetAddress.getLocalHost(), YZFS.CLIENT_PORT);

		/*
		 * the client need to open the listening socket first before send
		 * message to master to avoid race condition in file transfer
		 */
		FileTransferThread fileTransfer = new FileTransferThread(localFileFullPath);
		fileTransfer.start();

		CommunicationModule.sendMessage(masterIP, masterPort, request);
	}

	private class FileTransferThread extends Thread {

		public FileTransferThread(String localFileFullPath) {
			this.localFileFullPath = localFileFullPath;
		}

		public void run() {
			try {
				ServerSocket serverSocket = new ServerSocket(YZFS.CLIENT_PORT);
				Socket socket = null;
				// replication factor hard coded here
				int i = 0;
				while (i++ < 2) {
					socket = serverSocket.accept();

					File file = new File(localFileFullPath);
					FileInputStream fileInput = new FileInputStream(file);
					BufferedInputStream bufferedInput = new BufferedInputStream(fileInput);
					OutputStream out = socket.getOutputStream();

					byte[] buffer = new byte[4096];
					int length = -1;

					while ((length = bufferedInput.read(buffer)) > 0) {
						out.write(buffer, 0, length);
						out.flush();
					}

					socket.close();
				}
			} catch (Exception e) {

			}
		}

		private String localFileFullPath;
	}

	private InetAddress masterIP;
	private int masterPort;

}
