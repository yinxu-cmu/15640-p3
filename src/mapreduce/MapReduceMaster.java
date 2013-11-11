/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;


import dfs.YZFS;

/**
 * @author yinxu
 *
 */
public class MapReduceMaster {
	
	private static boolean ongoing = true;
	public static Queue<MapReduceTask> mapQueue;
	public static Queue<MapReduceTask> reduceQueue;
	
	public static void main(String[] args) {
		
		ServerSocket ss;
		
		try {
			ss = new ServerSocket(YZFS.MP_SLAVE_PORT);
			while(ongoing) {
				Socket sock = ss.accept();
				MapReduceMasterThread masterThread = new MapReduceMasterThread(sock);
				masterThread.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
