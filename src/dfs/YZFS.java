package dfs;

import java.io.IOException;
import java.net.UnknownHostException;

import exception.YZFSMasterServiceException;
import exception.YZFSSlaveServiceException;

public class YZFS {

	/* to run, go to bin/ directory, type java dfs.YZFS */
	public static void main(String[] args) throws YZFSMasterServiceException, UnknownHostException,
			IOException, ClassNotFoundException, InterruptedException {
		/* start master server */
		if (args.length == 0) {
			MasterServer ms = new MasterServer();
			ms.run();
			System.out.println("Master Serivce Ended");
			System.exit(0);
		}

		/* start background-running slave server */
		else if (args.length == 2 && args[0].equals("-c")) {
			SlaveServer ss = new SlaveServer();
			try {
				ss.startService(args[1]);
			} catch (YZFSSlaveServiceException e) {
				System.err.println("File System Slave Serivce Ended with Exception");
			}
			System.out.println("Slave Serivce Ended");
		}
		
		else if (args.length > 0 && args[0].equals("-yzfs")) {
			CommandLine commandLine = new CommandLine();
			commandLine.parseCommandLine(args);
		}

		else {
			System.out.println("Usage: java ProcessManager [-c <master hostname or ip>]");
		}
	}

	public static final int MASTER_PORT = 62742;
	public static final int SLAVE_PORT = 62743;
	public static final int CLIENT_PORT = 62744;
	
	public static final int replicationFactor = 2;
	
	public static final String fileSystemWorkingDir= "/tmp/YZFS/";
	public static final String localWorkingDir = "/tmp/LOCAL/";
	
	public static final int RECORD_LENGTH = 4;
	public static final int CHUNK_SIZE = 2; /* # of records per chunk */
	
	//11.10 added masterhost name as a global var
	public static final String MASTER_HOST = "localhost";
	//mapreduce slave port
	public static final int MP_SLAVE_PORT = 15444;

}
