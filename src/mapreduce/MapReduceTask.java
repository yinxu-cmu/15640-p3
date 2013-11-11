/**
 * 
 */
package mapreduce;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * @author yinxu
 * Base class for mapreduce task, should be extended to map task and reduce task
 *
 */
public class MapReduceTask implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4057027904752161050L;
	
	private String inputFileName; //specify input files
	private String outputFileName; //specify output files
	private char status; //specify the task status: done, running
	private int type; //specify the job is a map or a reduce, USELESS NOW!!
	public static final char DONE = 'd';
	public static final char RUNNING = 'r';
	public static final int MAP = 0;
	public static final int REDUCE = 1; // USELESS NOW!!
	
	private InetAddress target;
	
	public MapReduceTask(){}
	public MapReduceTask(String inputFileName, char status) {
		this.inputFileName = inputFileName;
		this.status = status;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}

	public char getStatus() {
		return status;
	}

	public void setStatus(char status) {
		this.status = status;
	}

	public InetAddress getTarget() {
		return target;
	}

	public void setTarget(InetAddress target) {
		this.target = target;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	
	
}
