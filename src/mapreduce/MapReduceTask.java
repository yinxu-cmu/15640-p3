/**
 * 
 */
package mapreduce;

import java.io.Serializable;

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
	
	private String[] inputFilesName; //specify input files
	private String[] outputFilesName; //specify output files
	private char status; //specify the task status: done, running
	public static final char DONE = 'd';
	public static final char RUNNING = 'r';
	
	public MapReduceTask(String[] inputFilesName, char status) {
		this.inputFilesName = inputFilesName;
		this.status = status;
	}

	public String[] getInputFilesName() {
		return inputFilesName;
	}

	public void setInputFilesName(String[] inputFilesName) {
		this.inputFilesName = inputFilesName;
	}

	public String[] getOutputFilesName() {
		return outputFilesName;
	}

	public void setOutputFilesName(String[] outputFilesName) {
		this.outputFilesName = outputFilesName;
	}

	public char getStatus() {
		return status;
	}

	public void setStatus(char status) {
		this.status = status;
	}

	
}
