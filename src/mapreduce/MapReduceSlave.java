/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import dfs.YZFS;

/**
 * @author yinxu
 * MapReduce slave need to run on each participant node. It receives MapReduceTask
 * and start the local job.
 * When starts, user need to specify the listening port number.
 *
 */
public class MapReduceSlave {
	
	/* To quit the framework, from any node type in the quit command, each 
	 * participant node will receive a msg containing the quit command.
	 */
	private static boolean ongoing = true;
	
	public static void main(String[] args) {
		
		if (args.length == 1 && args[0].equals("start")) {
			
			try { 
				ServerSocket dwldSocket = new ServerSocket(YZFS.MP_DOWNLOAD_PORT);
				ServerSocket ss = new ServerSocket(YZFS.MP_SLAVE_PORT);
				ObjectInputStream input;
				ObjectOutputStream output;
				while (ongoing) {
					Socket sock = ss.accept();
					//start to run the task. ??? Is thread needed here? yes
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

}
