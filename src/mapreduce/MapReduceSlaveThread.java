/**
 * 
 */
package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;

import example.Maximum;

import mapreduce.OutputCollector.Entry;

/**
 * @author yinxu This class is in charge of running the received MapReduceTask
 * 
 */
public class MapReduceSlaveThread extends Thread {

	public static void main(String[] args) throws InterruptedException {
		// System.out.println("sadf");
		MapReduceTask task = new MapReduceTask();

		task.setMapClass(Maximum.Map.class);
		task.setMapInputKeyClass(LongWritable.class);
		task.setMapInputValueClass(Text.class);
		task.setMapOutputKeyClass(Text.class);
		task.setMapOutputValueClass(LongWritable.class);

		task.setReduceClass(Maximum.Reduce.class);
		task.setReduceInputKeyClass(Text.class);
		task.setReduceInputValueClass(LongWritable.class);
		task.setReduceOutputKeyClass(Text.class);
		task.setReduceOutputValueClass(LongWritable.class);

		task.setType(MapReduceTask.MAP);

		task.setInputFileName(new String[]{"test4.txt"});
		MapReduceSlaveThread t4 = new MapReduceSlaveThread(task);
		t4.start();

		Thread.sleep(1500);

		task.setInputFileName(new String[]{"test5.txt"});
		MapReduceSlaveThread t5 = new MapReduceSlaveThread(task);
		t5.start();

		Thread.sleep(1500);

		task.setInputFileName(new String[]{"test6.txt"});
		MapReduceSlaveThread t6 = new MapReduceSlaveThread(task);
		t6.start();

		Thread.sleep(1500);

		task.setType(MapReduceTask.REDUCE);
		task.setInputFileName(new String[]{"test4.txt.out", "test5.txt.out", "test6.txt.out"});
		MapReduceSlaveThread t0 = new MapReduceSlaveThread(task);
		t0.start();
	}

	private MapReduceTask task;

	public MapReduceSlaveThread(MapReduceTask task) {
		this.task = task;
	}

	@Override
	public void run() {

		// if the task is a map task perform mapper
		if (task.getType() == MapReduceTask.MAP) {
			try {
				map(task);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (task.getType() == MapReduceTask.REDUCE) {
			try {
				reduce(task);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.err.println("Wrong Map Reduce Task Type!!!");
		}

		// send back ack reply

	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void map(MapReduceTask task) throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, InstantiationException,
			IllegalAccessException, InvocationTargetException, IOException {

		OutputCollector mapOutput = new OutputCollector();
		OutputCollector combineOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a mapper
		Constructor mapConstr = task.getMapClass().getConstructor(null);
		Object mapper = mapConstr.newInstance(null);

		// get a map method from the mapper
		Class<?>[] mapMethodClassArgs = {task.getMapInputKeyClass(), task.getMapInputValueClass(),
				OutputCollector.class, Reporter.class};
		Method mapMethod = task.getMapClass().getMethod("map", mapMethodClassArgs);

		// instatiate a inputValue
		Constructor inputValueConstr = task.getMapInputValueClass().getConstructor(null);
		Object inputValue = inputValueConstr.newInstance(null);

		// get a set method from the inputValue
		Method setInputValue = task.getMapInputValueClass().getMethod("set",
				new Class<?>[]{String.class});

		// invoke map method for every line of the input file
		BufferedReader bufferedReader = new BufferedReader(new FileReader(
				task.getInputFileName()[0]));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			setInputValue.invoke(inputValue, line);
			Object[] mapMethodObjectArgs = {null, inputValue, mapOutput, reporter};
			mapMethod.invoke(mapper, mapMethodObjectArgs);
		}

		// print out the result of map
		// System.out.println("debug");

		// instantiate a combiner
		Constructor combineConstr = task.getReduceClass().getConstructor(null);
		Object combiner = combineConstr.newInstance(null);

		// get a combine method from the combiner
		Class<?>[] combineMethodClassArgs = {task.getReduceInputKeyClass(), Iterator.class,
				OutputCollector.class, Reporter.class};
		Method combineMethod = task.getReduceClass().getMethod("reduce", combineMethodClassArgs);

		// start combining the map result
		mapreduce.OutputCollector.Entry entry = (Entry) mapOutput.queue.poll();
		mapreduce.OutputCollector.Entry tmpEntry = null;
		ArrayList values = new ArrayList();
		Iterator itrValues = null;

		Object key = entry.getKey();
		values.add(entry.getValue());

		Method method = key.getClass().getMethod("getHashcode", null);
		int hash = ((Integer) method.invoke(key, null));
		int tmpHash = 0;

		// invoke reduce method for every key-value pair that has the same key
		// hashcode
		while (mapOutput.queue.size() != 0) {
			tmpEntry = (Entry) mapOutput.queue.poll();
			tmpHash = ((Integer) method.invoke(tmpEntry.getKey(), null));
			if (tmpHash == hash) {
				values.add(tmpEntry.getValue());
			} else {
				itrValues = values.iterator();
				Object[] combineMethodObjectArgs = {key, itrValues, combineOutput, reporter};
				combineMethod.invoke(combiner, combineMethodObjectArgs);
				entry = tmpEntry;
				key = entry.getKey();
				hash = ((Integer) method.invoke(key, null));
				values = new ArrayList();
				values.add(entry.getValue());
			}
		}

		// don't forget the last one :)
		itrValues = values.iterator();
		Object[] combineMethodObjectArgs = {key, itrValues, combineOutput, reporter};
		combineMethod.invoke(combiner, combineMethodObjectArgs);

		// write the combine result into object file
		FileOutputStream fileOut = new FileOutputStream(task.getInputFileName()[0] + ".out");
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(combineOutput);

		// while (combineOutput.queue.size() != 0)
		// System.out.print(combineOutput.queue.poll() + "  ");
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

		File file = new File("output.txt");
		FileWriter fileWriter = new FileWriter(file);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		while (reduceOutput.queue.size() != 0) {
			// System.out.print(reduceOutput.queue.poll() + "  ");
			bufferedWriter.write(reduceOutput.queue.poll().toString() + "\n");
		}
		bufferedWriter.close();

	}

	public static ArrayList<Integer> getMinIndices(Entry[] entries, Method getHashcode)
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
}
