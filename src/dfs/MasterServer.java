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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.HashSet;

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

		/* test each individual function here */
//		catCommand("file1");
//		copyFromLocalCommand("/tmp/copy1");
//		copyToLocalCommand("copy1", "/tmp/");
//		lsCommand();
//		mkdirCommand();
//		rmCommand("file1");

		// while (true) {
		// try {
		// Socket socketServing = socketListener.accept();
		// BufferedReader in = new BufferedReader(new InputStreamReader(
		// socketServing.getInputStream()));
		// PrintWriter out = new PrintWriter(new OutputStreamWriter(
		// socketServing.getOutputStream()));
		//
		// /* save all the information into the list for future use */
		// SlaveInfo slaveInfo = new SlaveInfo();
		// slaveInfo.iaddr = socketServing.getInetAddress();
		// slaveInfo.port = socketServing.getPort();
		// synchronized (slaveList) {
		// slaveList.add(slaveInfo);
		// }
		//
		// System.out.println("Socket accepted from " +
		// socketServing.getInetAddress() + " "
		// + socketServing.getPort());
		// } catch (IOException e) {
		// System.err.println("Fail to accept slave server request.");
		// }
		// }
	}

	/**
	 * 
	 * @param localsrc (full path and file name) of the local file
	 * @return
	 */
	private boolean copyFromLocalCommand(String localsrc) {
		File srcFile = new File(localsrc);
		return copyFile(localsrc, this.directoryPath + srcFile.getName());
	}

	/**
	 * 
	 * @param remoteSrcFileName only the file name of the remote file
	 * @param localDesDirectory only the directory to which you want copy the file 
	 * @return
	 */
	private boolean copyToLocalCommand(String remoteSrcFileName, String localDesDirectory) {
		return copyFile(this.directoryPath + remoteSrcFileName, localDesDirectory
				+ remoteSrcFileName);
	}

	private boolean copyFile(String src, String des) {
		InputStream inStream = null;
		OutputStream outStream = null;

		try {
			File srcFile = new File(src);
			File desFile = new File(des);

			if (!srcFile.exists())
				return false;
			if (!desFile.exists())
				desFile.createNewFile();

			inStream = new FileInputStream(srcFile);
			outStream = new FileOutputStream(desFile);

			byte[] buffer = new byte[1024];
			int length;
			// copy the file content in bytes
			while ((length = inStream.read(buffer)) > 0) {
				outStream.write(buffer, 0, length);
			}

			inStream.close();
			outStream.close();

			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
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

	private String lsCommand() {
		File folder = new File(directoryPath);
		File[] listOfFiles = folder.listFiles();
		int numFiles = 0;
		StringBuilder ret = new StringBuilder();

		for (File file : listOfFiles) {
			if (file.isFile()) {
				ret.append(getAccessPermissions(file) + '\t' + file.length() + '\t'
						+ dateFormat.format(file.lastModified()) + '\t' + file.getName() + '\n');
				numFiles++;

			}
		}
		ret.insert(0, "Found " + numFiles + " items\n");

		System.out.print(ret);
		return ret.toString();
	}

	private String getAccessPermissions(File file) {
		StringBuilder ret = new StringBuilder();
		// file.canRead() ? ret.append('r') : ret.append('-');
		if (file.canRead())
			ret.append('r');
		else
			ret.append('-');

		if (file.canWrite())
			ret.append('w');
		else
			ret.append('-');

		if (file.canExecute())
			ret.append('x');
		else
			ret.append('-');

		return ret.toString();
	}

	private void mkdirCommand() {
		File folder = new File(this.directoryPath);
		if (!folder.exists()) {
			if (folder.mkdir()) {
				System.out.println("Directory is created!");
			} else {
				System.err.println("Failed to create directory!");
			}
		}
	}

	private boolean rmCommand(String fileName) {
		File file = new File(this.directoryPath + fileName);
		return file.delete();
	}

	private HashSet<SlaveInfo> slaveList = new HashSet<SlaveInfo>();
	private final String directoryPath = "/tmp/YZFS/";
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}
