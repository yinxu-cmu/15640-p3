package dfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import message.Message;

public class CommunicationModule {
	public static void sendMessage(InetAddress ipAddr, int port, Message msg)
			throws UnknownHostException, IOException, ClassNotFoundException {
		Message reply = null;
		Socket socket = null;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;

		/* open socket talk to the receiver */
		socket = new Socket(ipAddr, port);
		msg.setSrcIP(socket.getLocalAddress());
		msg.setSrcPort(socket.getLocalPort());
		output = new ObjectOutputStream(socket.getOutputStream());
		output.writeObject(msg);
		output.flush();

		// input = new ObjectInputStream(socket.getInputStream());
		// reply = (Message) input.readObject();
		//
		// return reply;
	}

	public static void sendMessage(Message msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		Message reply = null;
		Socket socket = null;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;

		/* open socket talk to the receiver */
		socket = new Socket(msg.getDesIP(), msg.getDesPort());
		output = new ObjectOutputStream(socket.getOutputStream());
		output.writeObject(msg);
		output.flush();

		// input = new ObjectInputStream(socket.getInputStream());
		// reply = (Message) input.readObject();
		//
		// return reply;
	}

	public static void sendMessage(Socket socket, Message msg) throws UnknownHostException,
			IOException, ClassNotFoundException {
		Message reply = null;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;

		/* open socket talk to the receiver */
		msg.setSrcIP(socket.getLocalAddress());
		msg.setSrcPort(socket.getLocalPort());
		output = new ObjectOutputStream(socket.getOutputStream());

		output.writeObject(msg);
		output.flush();

		// input = new ObjectInputStream(socket.getInputStream());
		// reply = (Message) input.readObject();
		// System.out.println("here???");
		// return reply;
	}

	public static void sendMessage(InputStream input, OutputStream output, Message msg) throws UnknownHostException,
			IOException, ClassNotFoundException {
		Message reply = null;
		ObjectOutputStream objOutput = new ObjectOutputStream(output);
//		ObjectInputStream objInput = new ObjectInputStream(input);

		/* open socket talk to the receiver */
//		msg.setSrcIP(socket.getLocalAddress());
//		msg.setSrcPort(socket.getLocalPort());
//		output = new ObjectOutputStream(socket.getOutputStream());

		objOutput.writeObject(msg);
		objOutput.flush();

		// input = new ObjectInputStream(socket.getInputStream());
		// reply = (Message) input.readObject();
		// System.out.println("here???");
		// return reply;
	}
}
