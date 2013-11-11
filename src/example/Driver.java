package example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.*;
import mapreduce.OutputCollector.Entry;

public class Driver {

	/**
	 * @param args
	 * @throws Throwable 
	 */
	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		Driver driver = new Driver();
		driver.map("test1.txt");
		driver.map("test2.txt");
		driver.map("test3.txt");
		driver.reduce(new String[] { "test1.txt.out", "test2.txt.out", "test3.txt.out" });
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	private void map(String input) throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, InstantiationException,
			IllegalAccessException, InvocationTargetException, IOException {

		OutputCollector mapOutput = new OutputCollector();
		OutputCollector combineOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a mapper
		Constructor mapConstr = mapClass.getConstructor(null);
		Object mapper = mapConstr.newInstance(null);

		// get a map method from the mapper
		Class<?>[] mapMethodClassArgs = { mapInputKeyClass, mapInputValueClass,
				OutputCollector.class, Reporter.class };
		Method mapMethod = mapClass.getMethod("map", mapMethodClassArgs);

		// instatiate a inputValue
		Constructor inputValueConstr = mapInputValueClass.getConstructor(null);
		Object inputValue = inputValueConstr.newInstance(null);

		// get a set method from the inputValue
		Method setInputValue = mapInputValueClass.getMethod("set", new Class<?>[] { String.class });

		BufferedReader bufferedReader = new BufferedReader(new FileReader(input));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			setInputValue.invoke(inputValue, line);
			Object[] mapMethodObjectArgs = { null, inputValue, mapOutput, reporter };
			mapMethod.invoke(mapper, mapMethodObjectArgs);
		}

		// instantiate a combiner
		Constructor combineConstr = reduceClass.getConstructor(null);
		Object combiner = combineConstr.newInstance(null);

		// get a combine method from the combiner
		Class<?>[] combineMethodClassArgs = { reduceInputKeyClass, Iterator.class,
				OutputCollector.class, Reporter.class };
		Method combineMethod = reduceClass.getMethod("reduce", combineMethodClassArgs);

		mapreduce.OutputCollector.Entry entry = (Entry) mapOutput.queue.poll();
		mapreduce.OutputCollector.Entry tmpEntry = null;
		ArrayList values = new ArrayList();
		Iterator itrValues = null;

		Object key = entry.getKey();
		values.add(entry.getValue());

		Method method = key.getClass().getMethod("getHashcode", null);
		int hash = ((Integer) method.invoke(key, null));
		int tmpHash = 0;
		while (mapOutput.queue.size() != 0) {
			tmpEntry = (Entry) mapOutput.queue.poll();
			tmpHash = ((Integer) method.invoke(tmpEntry.getKey(), null));
			if (tmpHash == hash) {
				values.add(tmpEntry.getValue());
			} else {
				itrValues = values.iterator();
				Object[] combineMethodObjectArgs = { key, itrValues, combineOutput, reporter };
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
		Object[] combineMethodObjectArgs = { key, itrValues, combineOutput, reporter };
		combineMethod.invoke(combiner, combineMethodObjectArgs);
		
		FileOutputStream fileOut = new FileOutputStream(input + ".out");
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(combineOutput);

//		while (combineOutput.queue.size() != 0)
//			System.out.print(combineOutput.queue.poll() + "  ");
	}

	@SuppressWarnings({ "unused", "rawtypes", "unchecked" })
	private void reduce(String[] inputs) throws Throwable {

		OutputCollector reduceOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a reducer
		Constructor reduceConstr = reduceClass.getConstructor(null);
		Object reducer = reduceConstr.newInstance(null);

		// get a reduce method from the reducer
		Class<?>[] reduceMethodClassArgs = { reduceInputKeyClass, Iterator.class,
				OutputCollector.class, Reporter.class };
		Method reduceMethod = reduceClass.getMethod("reduce", reduceMethodClassArgs);

		int size = inputs.length;
		OutputCollector[] reduceInputs = new OutputCollector[size];
		Entry[] entries = new Entry[size];

		FileInputStream fileIn = null;
		ObjectInputStream objIn = null;
		for (int i = 0; i < size; i++) {
			fileIn = new FileInputStream(inputs[i]);
			objIn = new ObjectInputStream(fileIn);
			reduceInputs[i] = ((OutputCollector) objIn.readObject());
			entries[i] = (Entry) reduceInputs[i].queue.poll();
		}

		Object key = entries[0].getKey();
		Method getHashcode = key.getClass().getMethod("getHashcode", null);
		ArrayList<Integer> minIndices = null;

		while ((minIndices = getMinIndices(entries, getHashcode)) != null) {
			key = entries[minIndices.get(0)].getKey();
			ArrayList values = new ArrayList();
			Iterator itrValues = null;

			for (int i : minIndices) {
				values.add(entries[i].getValue());
				entries[i] = (Entry) reduceInputs[i].queue.poll();
			}

			itrValues = values.iterator();
			// reducer.reduce((Text) key, itrValues, reduceOutput, reporter);
			Object[] reduceMethodObjectArgs = { key, itrValues, reduceOutput, reporter };
			reduceMethod.invoke(reducer, reduceMethodObjectArgs);
			
			FileOutputStream fileOut = new FileOutputStream("output");
			ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
			objOut.writeObject(reduceOutput);

			// while (reduceOutput.queue.size() != 0)
			// System.out.print(reduceOutput.queue.poll() + "  ");
		}

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

	// should get from conf file
	@SuppressWarnings("rawtypes")
	private Class mapClass = WordCount.Map.class;
	@SuppressWarnings("rawtypes")
	private Class mapInputKeyClass = LongWritable.class;
	@SuppressWarnings("rawtypes")
	private Class mapInputValueClass = Text.class;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class mapOutputKeyClass = Text.class;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class mapOutputValueClass = IntWritable.class;

	@SuppressWarnings("rawtypes")
	private Class reduceClass = WordCount.Reduce.class;
	@SuppressWarnings("rawtypes")
	private Class reduceInputKeyClass = Text.class;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class reduceInputValueClass = IntWritable.class;
	@SuppressWarnings({ "unused", "rawtypes" })
	private Class reduceOutputKeyClass = Text.class;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class reduceOutputValueClass = IntWritable.class;

}
