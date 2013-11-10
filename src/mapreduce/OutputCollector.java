package mapreduce;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class OutputCollector<OutputKey, OutputValue> {
	
	public HashMap<OutputKey, OutputValue> map = new HashMap<OutputKey, OutputValue>();

	public void collect(OutputKey outputKey, OutputValue outputValue) throws SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		// dirty clone here
//		Class<?> clazz = outputKey.getClass();
//		Constructor<?> constructor = clazz.getConstructor(null);
		Method method = outputKey.getClass().getMethod("clone", null);
		OutputKey outputKeyClone = (OutputKey) method.invoke(outputKey, null);
		map.put(outputKeyClone, outputValue);
	}

}
