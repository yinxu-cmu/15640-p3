/**
 * 
 */
package mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import message.Message;

import dfs.CommunicationModule;
import dfs.YZFS;

/**
 * @author yinxu MapReduce slave need to run on each participant node. It
 *         receives MapReduceTask and start the local job. When starts, user
 *         need to specify the listening port number.
 * 
 */
public class MapReduceSlave {

	/*
	 * To quit the framework, from any node type in the quit command, each
	 * participant node will receive a msg containing the quit command.
	 */
	private static boolean ongoing = true;

	public static void main(String[] args) throws FileNotFoundException, IOException {

		if (args.length == 1 && args[0].equals("start")) {

			ConnectMaster connectMaster = new ConnectMaster();
			connectMaster.start();

			try {
				ServerSocket dwldSocket = new ServerSocket(YZFS.MP_DOWNLOAD_PORT);
				ServerSocket ss = new ServerSocket(YZFS.MP_SLAVE_PORT);
				while (ongoing) {
					Socket sock = ss.accept();
					MapReduceSlaveThread slaveThread = new MapReduceSlaveThread(sock, dwldSocket);
					slaveThread.start();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("invalid arguments");
		}
	}

	/**
	 * The only thing it does is to tell master to add its ip to slavelist
	 * 
	 * @author zhengk
	 * 
	 */
	private static class ConnectMaster extends Thread {

		public ConnectMaster() throws FileNotFoundException, IOException {
			// load a properties file to read master ip and port
			Properties prop = new Properties();
			prop.load(new FileInputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"));
			this.masterHostName = InetAddress.getByName(prop.getProperty("master host name"));
		}

		public void run() {
			Message msg = new Message();
			msg.setFromMapReduceSlave(true);
			msg.setDes(masterHostName, YZFS.MASTER_PORT);

			try {
				CommunicationModule.sendMessage(msg);
			} catch (Exception e) {
				System.out.println("slave quit with master");
				System.exit(0);
			}
		}

		private InetAddress masterHostName = null;
	}

}
