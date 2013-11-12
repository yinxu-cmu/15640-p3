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

import dfs.SlaveInfo;
import dfs.YZFS;

import message.*;

/**
 * @author yinxu
 *
 */
public class MapReduceMasterThread extends Thread{

//	private Message msg;
//	private Socket sock;
	private ObjectInputStream input;
	private ObjectOutputStream output;
	
	public MapReduceMasterThread(Socket sock) {
		
		try {
			input = new ObjectInputStream(sock.getInputStream());
			output = new ObjectOutputStream(sock.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		
		Object msg = null;
		try {
			msg = (Object) input.readObject();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (msg instanceof NewJobMsg) {
			executeNewJob((NewJobMsg)msg);
		} else if (msg instanceof MapReduceTask) {
			ackTaskFinish((MapReduceTask)msg);
		}
		
	}
	
	public void executeNewJob(NewJobMsg msg) {
		String jobName = msg.getJobName(); // For future use
		
		/* split the job and generate list of tasks */
		//Get the input file names (socket to YZFS)
		Socket sockFS;
		try {
			sockFS = new Socket(YZFS.MASTER_HOST, YZFS.MASTER_PORT);
			ObjectOutputStream outputFS = new ObjectOutputStream(sockFS.getOutputStream());
			ObjectInputStream inputFS = new ObjectInputStream(sockFS.getInputStream());
			
			RequestFileMapMsg rfm = new RequestFileMapMsg();
			outputFS.writeObject(rfm);
			RequestFileMapMsg reply = (RequestFileMapMsg)inputFS.readObject();
			
			// generate task list
			generateTaskList(reply);
			System.out.println("new job:" + jobName + "has been added");
			
			/* send out tasks */
			sendOutTasks();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/* generate mapper task list */
	public void generateTaskList(RequestFileMapMsg msg) {
		HashMap<String, ArrayList<String>> fileToPart = msg.getFileToPart();
		HashMap<String, ArrayList<SlaveInfo>> partToSlave = msg.getPartToSlave();
		
		for(ArrayList<String> partList : fileToPart.values()) {
			for(String part : partList) {
				MapReduceTask task = new MapReduceTask();
				task.setInputFileName(new String[] {part});
				task.setOutputFileName(part + ".output");
				//alwasy choose the first candidate
				InetAddress target = partToSlave.get(part).get(0).iaddr;
				task.setTarget(target);
				task.setType(0);
				
				synchronized(MapReduceMaster.mapQueue) {
					MapReduceMaster.mapQueue.add(task);
				}
			}
		}
	}
	
	/* send out the MapReduceTask in the map queue */
	public void sendOutTasks() {
		
		System.out.println("sending out map tasks...");
		Socket sockTask;
		ObjectOutputStream outputTask;
		int taskCount = 0;
		
		while(!MapReduceMaster.mapQueue.isEmpty()) {
			
			MapReduceTask task;
			
			synchronized(MapReduceMaster.mapQueue) {
				task = MapReduceMaster.mapQueue.remove();
			}
			
			try {
				sockTask = new Socket(task.getTarget(), YZFS.MP_SLAVE_PORT);
				outputTask = new ObjectOutputStream(sockTask.getOutputStream());
				task.setStatus(MapReduceTask.RUNNING);
				outputTask.writeObject(task);
				System.out.println("sent task " + taskCount);
				taskCount++;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/* called when receive task finish message */
	public void ackTaskFinish(MapReduceTask task) {
		System.out.println("Finished task: " + task.getInputFileName());
		if (task.getStatus() == task.DONE) {
			/* whole job is done, yay! */
		} else {
			/*  */
			if (task.getType() == 0) {
				/* a map task is finished, master download the intermedia file */
				downloadMapResult(task);
			} else {
				/* a reduce task is finished */
				
			}
		}
	}
	
	/* Download the intermediate file of mapper, act as download server */
	public void downloadMapResult(MapReduceTask task) {
		
	}
}
