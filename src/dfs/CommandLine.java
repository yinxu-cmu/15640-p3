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
import java.util.Properties;

import message.AckMsg;
import message.CopyFromLocalCommandMsg;
import message.Message;

public class CommandLine {

	public void parseCommandLine(String[] args) throws FileNotFoundException, IOException,
			ClassNotFoundException {
		/* args[0] always equals to "-yzfs" */
		if (args[1].equals("copyFromLocal"))
			copyFromLocal(args[2]);
	}

	private void copyFromLocal(String localFileFullPath) throws FileNotFoundException, IOException,
			ClassNotFoundException {
		System.out.println("copy from local command line parsed");
		Properties prop = new Properties();

		// load a properties file
		prop.load(new FileInputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"));
		InetAddress masterIP = InetAddress.getByName(prop.getProperty("master host name"));
		int masterPort = Integer.parseInt(prop.getProperty("master port number"));
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
				while (true) {
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

}
