/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
		if (args.length == 2 && args[0].equals("-p")) {
			/*
			 * args[0]: -p
			 * args[1]: port #
			 */
			try {
				int port = Integer.parseInt(args[1]);
				ServerSocket ss = new ServerSocket(port);
				ObjectInputStream input;
				ObjectOutputStream output;
				while (ongoing) {
					Socket sock = ss.accept();
					input = new ObjectInputStream(sock.getInputStream());
					Object obj = input.readObject();
					
					if (obj instanceof MapReduceTask) {
						MapReduceTask task = (MapReduceTask) obj;
						//start to run the task. ??? Is thread needed here? yes
						MapReduceSlaveThread slaveThread = new MapReduceSlaveThread(task);
						slaveThread.start();
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("invalid arguments");
		}
	}

}
