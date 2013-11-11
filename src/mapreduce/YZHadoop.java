/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Queue;

import message.*;

import dfs.SlaveInfo;
import dfs.YZFS;

/**
 * @author yinxu
 * main YZHadoop program running on top of YZFS file system
 */
public class YZHadoop {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/* ??? haven't handle invalid args */
		if(args.length == 1) {
			/* args[0]: hadoop job name
			 * By defautl, take all files in YZFS as input files, and output
			 * to the root directory of YZFS as well. The default name is output
			 */
			NewJobMsg msg = new NewJobMsg(args[0]);
			try {
				Socket sock = new Socket(YZFS.MASTER_HOST, YZFS.MP_SLAVE_PORT);
				ObjectOutputStream output = new ObjectOutputStream(sock.getOutputStream());
				output.writeObject(msg);
				System.out.println("Sent out job: " + args[0]);
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			System.out.println("invalid arguments");
		}

	}

}
