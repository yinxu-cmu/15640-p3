/**
 * 
 */
package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import dfs.SlaveInfo;
import dfs.YZFS;
import example.Maximum;

import mapreduce.OutputCollector.Entry;
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
//		NewJobMsg msg = null;
		try {
			System.out.println("Reading object from socket");
			msg = (Object) input.readObject();
//			msg = (NewJobMsg) input.readObject();
//			System.out.println(msg.getJobName());
			AckMsg ack = new AckMsg(true);
			output.writeObject(ack);
			output.flush();
			
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
		System.out.println("enterred executeNewJob: "+jobName);
		
		int jobId = MapReduceMaster.jobId.incrementAndGet();
		
		synchronized (MapReduceMaster.jobToTaskCount) {
			MapReduceMaster.jobToTaskCount.put(jobId, 0);
		}
		
		/* split the job and generate list of tasks */
		//Get the input file names (socket to YZFS)
		Socket sockFS;
		try {
			sockFS = new Socket(YZFS.MASTER_HOST, YZFS.MASTER_PORT);
			System.out.println("1");
			ObjectOutputStream outputFS = new ObjectOutputStream(sockFS.getOutputStream());
			System.out.println("2");
			System.out.println("3");
			
			System.out.println("request filemap...");
			RequestFileMapMsg rfm = new RequestFileMapMsg();
			outputFS.writeObject(rfm);
			outputFS.flush();
			
			ObjectInputStream inputFS = new ObjectInputStream(sockFS.getInputStream());
			RequestFileMapMsg reply = (RequestFileMapMsg)inputFS.readObject();
			
			System.out.println("request filemap done.");
			// generate task list
			generateTaskList(reply, jobName, jobId);
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
	public void generateTaskList(RequestFileMapMsg msg, String jobName, int jobId)
			throws ClassNotFoundException {
		HashMap<String, ArrayList<String>> fileToPart = msg.getFileToPart();
		HashMap<String, ArrayList<SlaveInfo>> partToSlave = msg.getPartToSlave();
		
		for(ArrayList<String> partList : fileToPart.values()) {
			for(String part : partList) {
				MapReduceTask task = new MapReduceTask();
				task.setInputFileName(new String[] {part});
				task.setOutputFileName(part + ".output");
				//always choose the first candidate
				InetAddress target = partToSlave.get(part).get(0).iaddr;
				task.setTarget(target);
				task.setType(0);
				task.setJobId(jobId);
				
				Class<?> c = Class.forName(jobName);
				task.setMapClass(c);
				task.setMapInputKeyClass(LongWritable.class); // has to be hardcoding here?
				task.setMapInputValueClass(Text.class);
				task.setMapOutputKeyClass(Text.class);
				task.setMapOutputValueClass(LongWritable.class);

				task.setReduceClass(c);
				task.setReduceInputKeyClass(Text.class);
				task.setReduceInputValueClass(LongWritable.class);
				task.setReduceOutputKeyClass(Text.class);
				task.setReduceOutputValueClass(LongWritable.class);
				
				synchronized(MapReduceMaster.mapQueue) {
					MapReduceMaster.mapQueue.add(task);
				}
				synchronized(MapReduceMaster.jobToTaskCount) {
					MapReduceMaster.jobToTaskCount.put(jobId, MapReduceMaster.jobToTaskCount.get(jobId)+1);
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
				outputTask.flush();
				
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
		int jobCount = 0;
		if (task.getStatus() != MapReduceTask.ERROR) {
			
			synchronized(MapReduceMaster.jobToTaskCount) {
				jobCount = MapReduceMaster.jobToTaskCount.get(task.getJobId());
				if (jobCount == 1) {
					System.out.println("All mapper tasks finshed.");
					//indicating all mappers are done
					System.out.println("starting reducer...");
					try {
						setReduceInputFile(task);
						reduce(task);
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					//jobCount-1
					MapReduceMaster.jobToTaskCount.put(task.getJobId(), jobCount-1);
				}
			}
			
		} else {
			System.out.println("Error happened");
		}
		
	}
	
	@SuppressWarnings({"unused", "rawtypes", "unchecked"})
	private void reduce(MapReduceTask task) throws Throwable {

		OutputCollector reduceOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a reducer
		Constructor reduceConstr = task.getReduceClass().getConstructor(null);
		Object reducer = reduceConstr.newInstance(null);

		// get a reduce method from the reducer
		Class<?>[] reduceMethodClassArgs = {task.getReduceInputKeyClass(), Iterator.class,
				OutputCollector.class, Reporter.class};
		Method reduceMethod = task.getReduceClass().getMethod("reduce", reduceMethodClassArgs);

		// read reduce inputs obj from map result obj
		int size = task.getInputFileName().length;
		OutputCollector[] reduceInputs = new OutputCollector[size];
		Entry[] entries = new Entry[size];

		FileInputStream fileIn = null;
		ObjectInputStream objIn = null;
		for (int i = 0; i < size; i++) {
			fileIn = new FileInputStream(task.getInputFileName()[i]);
			objIn = new ObjectInputStream(fileIn);
			reduceInputs[i] = ((OutputCollector) objIn.readObject());
			entries[i] = (Entry) reduceInputs[i].queue.poll();
		}

		// get getHashcode method from key obj
		Object key = entries[0].getKey();
		Method getHashcode = key.getClass().getMethod("getHashcode", null);
		ArrayList<Integer> minIndices = null;

		// start the merge sort
		while ((minIndices = getMinIndices(entries, getHashcode)) != null) {
			key = entries[minIndices.get(0)].getKey();
			ArrayList values = new ArrayList();
			Iterator itrValues = null;

			// add every value (that has the least key hash value) into the
			// value list
			for (int i : minIndices) {
				values.add(entries[i].getValue());
				entries[i] = (Entry) reduceInputs[i].queue.poll();
			}

			// invoke reduce method
			itrValues = values.iterator();
			Object[] reduceMethodObjectArgs = {key, itrValues, reduceOutput, reporter};
			reduceMethod.invoke(reducer, reduceMethodObjectArgs);

		}

		File file = new File("/tmp/YZFS/output.txt");
//		File file = new File("/YZFS/output.txt");
		FileWriter fileWriter = new FileWriter(file);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		while (reduceOutput.queue.size() != 0) {
			// System.out.print(reduceOutput.queue.poll() + "  ");
			bufferedWriter.write(reduceOutput.queue.poll().toString() + "\n");
		}
		bufferedWriter.close();

	}
	
	public ArrayList<Integer> getMinIndices(Entry[] entries, Method getHashcode)
			throws Throwable, NoSuchMethodException {
		ArrayList<Integer> ret = null;
		int length = entries.length;
		int minHash = Integer.MAX_VALUE;

		for (int i = 0; i < length; i++) {
			if (entries[i] == null)
				continue;

			int hash = ((Integer) getHashcode.invoke(entries[i].getKey(), null));
			if (hash < minHash) {
				minHash = hash;
				ret = new ArrayList<Integer>();
				ret.add(i);
			} else if (hash == minHash) {
				ret.add(i);
			}
		}

		return ret;
	}
	
	public void setReduceInputFile(MapReduceTask task) {
		File[] files = new File("/tmp/YZFS/").listFiles();
//		File[] files = new File("/YZFS/").listFiles();
		String[] inputFiles = new String[files.length];
		for (int i = 0; i < inputFiles.length; i++) {
			inputFiles[i] = files[i].toString();
		}
//		task.setInputFileName(new String[]{"test4.txt.out", "test5.txt.out", "test6.txt.out"});
		task.setInputFileName(inputFiles);
	}
	
//	public static void main(String[] args) throws Throwable {
//		MapReduceTask task = new MapReduceTask();
//		
//				task.setMapClass(Maximum.Map.class);
//				task.setMapInputKeyClass(LongWritable.class);
//				task.setMapInputValueClass(Text.class);
//				task.setMapOutputKeyClass(Text.class);
//				task.setMapOutputValueClass(LongWritable.class);
//		
//				task.setReduceClass(Maximum.Reduce.class);
//				task.setReduceInputKeyClass(Text.class);
//				task.setReduceInputValueClass(LongWritable.class);
//				task.setReduceOutputKeyClass(Text.class);
//				task.setReduceOutputValueClass(LongWritable.class);
//			
//		setReduceInputFile(task);
//		reduce(task);
//	}
	
	
}
