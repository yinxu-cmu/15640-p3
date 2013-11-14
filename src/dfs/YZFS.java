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
			System.out.println("Master Serivce Ended");
			System.exit(0);
		}

		/* start background-running slave server */
		else if (args.length == 2 && args[0].equals("-c")) {
			SlaveServer ss = new SlaveServer(args[1]);
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

	public static final int MASTER_PORT = 62762;
	public static final int SLAVE_PORT = 62763;
	public static final int CLIENT_PORT = 62764;
	
	public static final int replicationFactor = 2;
	
	public static final String fileSystemWorkingDir= "/tmp/YZFS/";
	
	public static final int RECORD_LENGTH = 4;
	public static final int NUM_RECORDS = 100000; /* # of records per chunk */
	
	//mapreduce slave port
	public static final int MP_SLAVE_PORT = 62765;
	public static final int MP_DOWNLOAD_PORT = 62766;

}
