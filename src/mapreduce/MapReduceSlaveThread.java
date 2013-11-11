/**
 * 
 */
package mapreduce;

/**
 * @author yinxu
 * This class is in charge of running the received MapReduceTask
 *
 */
public class MapReduceSlaveThread extends Thread{
	
	private MapReduceTask task;
	
	public MapReduceSlaveThread(MapReduceTask task) {
		this.task = task;
	}
	
	@Override
	public void run() {
		/* ??? now just assume it is a map task */
		String[] inputFileNames = task.getInputFilesName();
		
	}
}
