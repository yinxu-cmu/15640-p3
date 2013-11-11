/**
 * 
 */
package mapreduce;

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
			try {
				Class c = Class.forName(args[0]);
				Object obj = c.newInstance();
				
				/* split the job and generate list of tasks */
				//TO DO
				
				/* send out tasks */
				//TO DO
				
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			System.out.println("invalid arguments");
		}

	}

}
