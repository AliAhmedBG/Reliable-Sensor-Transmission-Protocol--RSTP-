/*
 * Replace the following string of 0s with your student number
 * 230071010
 */
import java.io.*;
import java.util.*;
import java.net.*;

public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server

	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/* 
	 * This method sends protocol metadata to the server.
	 * See coursework specification for full details.	
	 */
	public void sendMetadata()   {
		// variable which holds the number of lines in the csv file
		int lineCount = 0;

		// creates a BufferedReader to read the contents of the input file line by line
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
			// increments the lineCount variable if there is another line until there are no more
			while (reader.readLine() != null) {
				lineCount++;
			}

			// fileTotalReadings is set to be the number of lines in the csv file
			fileTotalReadings = lineCount;

			// outputs to the user the number of lines in the file
			System.out.println("Client: Total numer of lines = " + fileTotalReadings);
		}

		// handles errors by first printing out that there is one and then printing the stack trace and closing the program
		catch (IOException e) {
			System.out.println("Client: Error reading file");
			e.printStackTrace();
			socket.close();
			System.exit(0);
		}

		// attaches the number of lines, the output file name and size to a string variable spererated by commas
		String payLoad = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;

		// instantiates a segment object with all the parameters made above
		Segment metaSegment = new Segment(0, SegmentType.Meta, payLoad, payLoad.length());

		try {
			// creates an output stream which collects the bytes and wraps it in an objectOutputstream
			ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
			objectOutputStream.writeObject(metaSegment);

			// the data variable now contains all the bytes that represent the metadata
			byte[] data = byteOutputStream.toByteArray();

			// create a packet containing the byte array to send to the server
			DatagramPacket packet = new DatagramPacket(data, data.length, ipAddress, portNumber);

			// sends the packet to the server
			socket.send(packet);

			System.out.println("Client: Metadata sent successfully");
		}
		catch (IOException e) {
			System.out.println("Client: Error sending metadata");
		}
	} 


	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	public void readAndSend() {
		// reads the file line by line
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {

			// skips lines that have already been sent
			for (int i = 0; i < sentReadings; i++) {
				reader.readLine();
			}

			//creates an array to hold the current patch and holds the current line being read and a counter for the line
			List<String> curPatch = new ArrayList<>();
			String curLine;
			int count = 0;

		/*loops through the rest of the file while setting curline to the line currently being read and adding it to
		the array until maxpatch is reached
		 */
			while ((curLine = reader.readLine()) != null && count < maxPatchSize) {
				curPatch.add(curLine);
				count++;
			}

			// adds the contents of curpatch to a payload variable and seperates all contents using a ;
			String payLoad = String.join(";", curPatch);

			// checks if the patch is empty and the following message is sent if it is
			if (curPatch.isEmpty()) {
				System.out.println("Client: No more data to send.");
				System.out.println("Client: All data has been successfully transmitted and acknowledged. Exiting.");
				socket.close();
				System.exit(0);

			}

			// sets up the segment of type data
			int seqNum = totalSegments % 2;
			dataSeg = new Segment(seqNum, SegmentType.Data, payLoad, payLoad.length());
			// updates counter
			totalSegments++;

			// similarly to the metadata segment sending
			ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
			objectOutputStream.writeObject(dataSeg);

			byte[] data = byteOutputStream.toByteArray();

			DatagramPacket packet = new DatagramPacket(data, data.length, ipAddress, portNumber);
			socket.send(packet);

			// outouts data about what the client has sent out for debug purposes
			System.out.println("CLient: Send: DATA [SEQ#" + seqNum + "](" + "Size:" + dataSeg.getSize() + ", Checksum:" +
					dataSeg.getChecksum() + ", Content:" + dataSeg.getPayLoad() + ")");

			// increases sent readings by the patch size so that messages arent resent
			sentReadings += curPatch.size();
		} catch (IOException e) {
			System.out.println("Client: Error sending data segment");
			e.printStackTrace();
		}
	}





		/*
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() {
		// waits for sservers ack before sending next batch
		try {
			// prepares bytes array and datagrampacket
			byte[] buffer = new byte[Protocol.MAX_Segment_SIZE];
			DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

			// blocks until the server sends something
			socket.receive(packet);

			// wrap bytes into streams to turn them back into objects
			ByteArrayInputStream byteInputStream = new ByteArrayInputStream(packet.getData());
			ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream);
			Segment ackSeg = (Segment) objectInputStream.readObject();

			// first checks if the segment type is ACk
			if (ackSeg.getType() == SegmentType.Ack && ackSeg.getSeqNum() == dataSeg.getSeqNum()) {
				// if so then return info on the ack and return ttrue
				System.out.println("Client: Receive: ACK [SEQ#" + ackSeg.getSeqNum() + "]");
				System.out.println("\t\t>>>>>>> NETWORK: ACK received successfully <<<<<<<<<");
				System.out.println("----------------------------------------------------");
				return true;
			}
			else {
				// if not then print an error and return false
				System.out.println("Client: Error — invalid ACK or sequence mismatch");
				return false;
			}
		}
		// throws an exception if there was an error receiving an ack
		catch (IOException | ClassNotFoundException e) {
			System.out.println("Client: Error receiving ACK");
			e.printStackTrace();
			return false;
		}
	}

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission()   {
		try {
			// sets the timeout
			socket.setSoTimeout(timeout);
			currRetry = 0;

			// runs constantly
			while (true) {
				try {
					// tries to receive the aknowledgement for the last segment
					if (receiveAck()) {
						// resets retry counter and stop waiting
						currRetry = 0;
						break;
					}
					else {
						// received an incorrect ack so incremenets retry
						currRetry++;
						if (currRetry > maxRetries) {
							// exits if maxretries has been reached
							System.out.println("Client: ERROR — maximum retries reached. Exiting.");
							System.exit(0);
						}

						// if the ack is ivalid it will try to resend it
						System.out.println("Client: Invalid ACK — resending segment again (retry " + currRetry + ")");

						ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
						ObjectOutputStream objectOutputStream= new ObjectOutputStream(byteOutputStream);

						objectOutputStream.writeObject(dataSeg);

						byte[] bytes = byteOutputStream.toByteArray();

						DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ipAddress, portNumber);
						socket.send(packet);
						totalSegments++;
					}

				}
				catch (SocketTimeoutException e) {
					// happens when it times out before and ack is received
					currRetry++;
					// exits if max retries has been reached
					if (currRetry > maxRetries) {
						System.out.println("Client: TIMEOUT ALERT — maximum retries reached. Exiting...");
						System.exit(0);
					}

					// resends segment when timeout occurs
					System.out.println("Client: TIMEOUT ALERT");
					System.out.println("Client: Resending the same segment again, current retry " + currRetry);

					ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
					objectOutputStream.writeObject(dataSeg);
					byte[] bytes = byteOutputStream.toByteArray();

					DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ipAddress, portNumber);
					socket.send(packet);
					totalSegments++;

				}
			}
		}
		// final catch for if an error occurs during timeout and retransmissions
		catch (IOException e) {
			System.out.println("Client: Error during timeout and retransmission");
			e.printStackTrace();
		}
	}


	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
		// keeps track of efficiency
		int totalBytes = 0;
		int usefulBytes = 0;

		// local list that will contain the readings
		List<String> receivedLines = new ArrayList<>();
		int readingCount = 0;


		// detects duplicates
		int expectedSeqNum = 0;
		Segment lastAck = null;

		try {
			// sets a timeout
			serverSocket.setSoTimeout(2000);

			// starts a constant loop
			while (true) {
				//sets up a buffer for receiving
				byte[] buffer = new byte[MAX_Segment_SIZE];
				DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

				try {
					// attempts to receive the packet
					serverSocket.receive(packet);
				}
				catch (SocketTimeoutException e) {
					System.out.println("Server: Timeout waiting for new Data segment. Exiting server.");
					break;
				}

				// turns the packet back into a Segment
				ByteArrayInputStream byteInputStream = new ByteArrayInputStream(packet.getData());
				ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream);
				Segment dataSeg = (Segment) objectInputStream.readObject();

				totalBytes += dataSeg.getSize();

				// outputs data on the packet
				System.out.println("Server: Receive: DATA [SEQ#" + dataSeg.getSeqNum() + "] (Size:" +
						dataSeg.getSize() + ", Checksum:" + dataSeg.getChecksum() + ", Content:" +
						dataSeg.getPayLoad() + ")");

				// validates the checksum before processing
				if (!dataSeg.isValid()) {
					System.out.println("Server: Invalid checksum detected, segment discarded.");
					continue;
				}

				// checks if the data segment is a duplicate
				if (dataSeg.getSeqNum() == expectedSeqNum) {
					usefulBytes += dataSeg.getSize();

					// extract and store readings from the received payload
					String payload = dataSeg.getPayLoad();
					if (payload != null && !payload.isEmpty()) {
						String[] readings = payload.split(";");
						Collections.addAll(receivedLines, readings);
					}

					// creates the ack
					Segment ack = new Segment(dataSeg.getSeqNum(), SegmentType.Ack, "", 0);
					lastAck = ack;

					// randomly decides if this ack is lost
					if (isLost(loss)) {
						System.out.println("Server: Simulating ACK loss. ACK[SEQ#" + dataSeg.getSeqNum() + "] is lost.");
					}
					else {
						Server.sendAck(serverSocket, packet.getAddress(), packet.getPort(), dataSeg.getSeqNum());
						System.out.println("Server: Send: ACK [SEQ#" + ack.getSeqNum() + "]");
						System.out.println("\t\t>>>>>>> NETWORK: ACK sent successfully <<<<<<<<<");
					}

					// alternates between 0 and 1 refernced from geeksforgeeks
					expectedSeqNum = (expectedSeqNum == 1) ? 0 : 1;
				}
				else {
					// tells the user that duplicate data was received
					System.out.println("Duplicate data is detected");
					System.out.println("Sending an Ack of the previous segment");

					// checks if lastAck is null and acts accordingly if so
					if (lastAck != null) {
						if (isLost(loss)) {
							System.out.println("Server: Simulating ACK loss. ACK[SEQ#" + lastAck.getSeqNum() + "] is lost.");
						} else {
							Server.sendAck(serverSocket, packet.getAddress(), packet.getPort(), lastAck.getSeqNum());
							System.out.println("Server: Send: ACK [SEQ#" + lastAck.getSeqNum() + "]");
							System.out.println("\t\t>>>>>>> NETWORK: ACK is sent successfully <<<<<<<<<");
						}
					} else {
						System.out.println("Server: No previous ACK to resend (this shouldn't normally happen)");
					}
				}
				System.out.println("----------------------------------------------------");
			}

			// write all received lines to the server's output file
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(instance.outputFileName))) {
				for (String line : receivedLines) {
					writer.write(line);
					writer.newLine();
				}
				System.out.println("Server: Data successfully written to " + instance.outputFileName);
			} catch (IOException e) {
				System.out.println("Server: Error writing received data to file.");
				e.printStackTrace();
			}


			// outputs info to the user about the file written to, total bytes etc
			System.out.println("Server: Data written to " + instance.outputFileName);
			System.out.println("Total Bytes: " + totalBytes);
			System.out.println("Useful Bytes: " + usefulBytes);
			System.out.println("Efficiency: " + ((float) usefulBytes / totalBytes) * 100 + " %");

		}
		// throws an exception for if theres an error receiving the data segment
		catch (IOException | ClassNotFoundException e) {
			System.out.println("Server: Error receiving Data segment");
			e.printStackTrace();
		}

	}


	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
