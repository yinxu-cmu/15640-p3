package mapreduce;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.PriorityQueue;

public class OutputCollector<OutputKey, OutputValue> {

	public OutputCollector() {
		
	}
	
	public void collect(OutputKey outputKey, OutputValue outputValue) throws SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// dirty clone here
		Method method = outputKey.getClass().getMethod("clone", null);
		OutputKey outputKeyClone = (OutputKey) method.invoke(outputKey, null);
		Entry entry = new Entry(outputKeyClone, outputValue);
		queue.add(entry);
	}
	
	public class Entry<Key, Value> implements Comparable{
		private Key key;
		private Value value;
		
		public Entry(Key key, Value value) {
			this.key = key;
			this.value = value;
		}
		
		public Key getKey() {
			return this.key;
		}
		
		public Value getValue() {
			return this.value;
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			int thisHash = 0, thatHash = 0;
			try {
				Method method = key.getClass().getMethod("getHashcode", null);
				thisHash = (Integer) method.invoke(key, null);
				thatHash = (Integer) method.invoke(((Entry)o).getKey(), null);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			System.out.println("this " + this.key + " hashcode: " + thisHash + 
//					" o " + ((Entry)o).getKey() + " hashcode: " + thatHash);
			return thisHash - thatHash;
		}
		
		public String toString() {
			return key.toString();
		}
	}

	public PriorityQueue<Entry> queue = new PriorityQueue<Entry>();
	
}
