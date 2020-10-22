/*
Copyright 2020 Erigo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*

CT2Arrow

Read data from a CT source and write it as record batches to either an Arrow output file or to a
Plasma in-memory object store. Each Arrow output file or Plasma object will contain one
record batch.

Channels for PHM08 data:
{"unit.i32","time.i32","op1.f32","op2.f32","op3.f32","sensor01.f32","sensor02.f32","sensor03.f32","sensor04.f32","sensor05.f32","sensor06.f32","sensor07.f32","sensor08.f32","sensor09.f32","sensor10.f32","sensor11.f32","sensor12.f32","sensor13.f32","sensor14.f32","sensor15.f32","sensor16.f32","sensor17.f32","sensor18.f32","sensor19.f32","sensor20.f32","sensor21.f32"}

The Arrow channel names are derived by removing the suffix from the CT channel names.
The Arrow channel types are derived by examining the suffix of each CT channel name.

John Wilson, Erigo Technologies

This application is based on the code snippets found at https://arrow.apache.org/docs/java/ipc.html

Other useful URLs
Javadoc for Arrow API is found at https://arrow.apache.org/docs/java/reference/
Python Plasma API https://arrow.apache.org/docs/python/plasma.html
https://www.infoq.com/articles/apache-arrow-java/
https://github.com/animeshtrivedi/blog/blob/master/post/2017-12-26-arrow.md
https://github.com/animeshtrivedi/ArrowExample/blob/master/src/main/java/com/github/animeshtrivedi/arrowexample/ArrowWrite.java

 */

package erigo.ct2arrow;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;

import cycronix.ctlib.CTdata;
import cycronix.ctlib.CTmap;
import cycronix.ctlib.CTreader;

import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.commons.cli.*;

import org.apache.arrow.memory.*;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;

public class CT2Arrow {

	// Data types
	public enum DataType {INT_DATA, FLOAT_DATA, DOUBLE_DATA, STRING_DATA}

	// Map containing all of the DataContainer objects
	LinkedHashMap<String,DataContainer> hashMap = new LinkedHashMap<>();

	CTreader ctr = null;

	// Time (msec) between flushing data to Arrow file or Plasma
	int flushPeriod_msec = 60000;

	// How much data to request when determining the next timestamp.
	// Good to keep this value smaller when walking through an existing CT source
	// and larger when reading live data.
	double next_timestamp_dur_sec = 1000000.0;

	// Run in debug mode?
	boolean bDebug = false;

	// Write data to Plasma? If this is false, write to Arrow file.
	boolean bPlasma = false;

	// How many record batches we have written out?
	int batchNum = 0;

	// Input source name
	// This is a command-line argument. Leave off the "CTdata" root folder.
	String ct_sourceName = null;

	String[] ct_chanNames = null;
	String[] arrow_chanNames = null;
	DataType[] chanDataTypes = null;

	// Create a separate container to hold CT timestamps
	DoubleDataContainer ct_timestamp_dc = null;

	String triggerChan = null;

	//
	// Main function
	//
	public static void main(String[] argsI) {
		try {
			new CT2Arrow(argsI);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e);
		}
	}

	//
	// CT2Arrow constructor
	// Everything happens in this method
	//
	public CT2Arrow(String[] argsI) throws Exception {

		//
		// Argument processing using Apache Commons CLI
		//
		// 1. Setup command line options
		Options options = new Options();
		options.addOption("h", "help", false, "Print this message.");
		options.addOption(Option.builder("s").argName("source name").hasArg().desc("Name of the CloudTurbine source to read data from; this source name can be up to 13 characters long.").build());
		options.addOption(Option.builder("chans").argName("channel name(s)").hasArg().desc("Comma-separated list of channel names; supported channel name suffixes and their associated data types: .txt (string), .i32 (32-bit integer), .f32 (32-bit floating point), .f64 (64-bit floating point).").build());
		options.addOption(Option.builder("f").argName("flush time").hasArg().desc("Flush interval (msec); specifies amount of time between flushing data to Arrow file or Plasma object; must be an integer greater than or equal to 0; default = " + Integer.toString(flushPeriod_msec) + ".").build());
		options.addOption(Option.builder("t").argName("trigger channel").hasArg().desc("Data will be flushed to Arrow file or Plasma object when the value of this CloudTurbine input channel changes. Periodic flush is still used as a secondary flushig mechanism. The specified channel must be one of the CloudTurbine input channels and it must have a \".i32\" extension.").build());
		options.addOption(Option.builder("d").argName("next timestamp duration").hasArg().desc("How much data (in seconds) to request when determining the next timestamp. Good to keep this value smaller when walking through an existing CT source and larger when reading live data; default = " + Double.toString(next_timestamp_dur_sec)).build());
		options.addOption("p", "plasma", false, "Write data to a Plasma object store; without this option (i.e. by default) output is written to Arrow file.");
		options.addOption("x", "debug", false, "Debug mode.");

		// 2. Parse command line options
		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try {	line = parser.parse( options, argsI );	}
		catch( ParseException exp ) {	// oops, something went wrong
			System.err.println( "Command line argument parsing failed: " + exp.getMessage() );
			return;
		}

		// 3. Retrieve the command line values
		if (line.hasOption("help")) {			// Display help message and quit
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp( "CT2Arrow", options );
			return;
		}

		if (!line.hasOption("s")) {
			System.err.println("Error: you must specify a CloudTurbine source name");
			return;
		}
		ct_sourceName = line.getOptionValue("s",ct_sourceName);
		if (ct_sourceName.length() > 13) {
			throw new Exception("CT source name is too long; must be 13 characters at most");
		}

		if (!line.hasOption("chans")) {
			System.err.println("Error: you must specify a comma-separated list of channel names");
			return;
		}
		String chanNameL = line.getOptionValue("chans");
		ct_chanNames = chanNameL.split(",");
		// Generate arrow_chanNames and chanDataTypes from ct_chanNames
		arrow_chanNames = new String[ct_chanNames.length];
		chanDataTypes = new DataType[ct_chanNames.length];
		// Make sure that the channel names use one of the accepted suffix: .txt, .i32, .f32, .f64
		for(int i=0; i<ct_chanNames.length; ++i) {
			int dotIdx = ct_chanNames[i].lastIndexOf('.');
			if ( (dotIdx < 0) || ( (dotIdx > -1) && (!ct_chanNames[i].endsWith(".txt")) && (!ct_chanNames[i].endsWith(".i32")) && (!ct_chanNames[i].endsWith(".f32")) && (!ct_chanNames[i].endsWith(".f64")) ) ) {
				System.err.println("Error: illegal channel name specified in the \"-chans\" list: " + ct_chanNames[i]);
				System.err.println("\tMust have one of the accepted suffixes: .txt, .i32, .f32, or .f64");
				return;
			}
			// For the Arrow channels name, remove the suffix
			arrow_chanNames[i] = ct_chanNames[i].substring(0,dotIdx);
			// Determine the data type from the suffix
			if (ct_chanNames[i].endsWith(".txt")) {
				chanDataTypes[i] = DataType.STRING_DATA;
			} else if (ct_chanNames[i].endsWith(".i32")) {
				chanDataTypes[i] = DataType.INT_DATA;
			} else if (ct_chanNames[i].endsWith(".f32")) {
				chanDataTypes[i] = DataType.FLOAT_DATA;
			} else if (ct_chanNames[i].endsWith(".f64")) {
				chanDataTypes[i] = DataType.DOUBLE_DATA;
			}
		}
		// Check that the input arrays are all the same size
		if ( (ct_chanNames.length != arrow_chanNames.length) || (ct_chanNames.length != chanDataTypes.length) ) {
			throw new Exception("Channel name size mismatch");
		}

		if (line.hasOption("t")) {
			triggerChan = line.getOptionValue("t");
			// This channel must be one of the input CT channels
			// Also, as a temporary limitation, this must be a ".i32" channel
			boolean bValidChan = false;
			for (int i = 0; i < ct_chanNames.length; ++i) {
				if ( (triggerChan.equals(ct_chanNames[i])) && (triggerChan.endsWith(".i32")) ) {
					bValidChan = true;
					break;
				}
			}
			if (!bValidChan) {
				System.err.println("Error: the trigger channel must be one of the CloudTurbine input channels and it must have a \".i32\" extension.");
				return;
			}
		}

		try {
			flushPeriod_msec = Integer.parseInt(line.getOptionValue("f", "" + flushPeriod_msec));
		} catch (NumberFormatException nfe) {
			System.err.println("Error: the flush period must be an integer greater than or equal to 0");
			return;
		}
		if (flushPeriod_msec < 0) {
			System.err.println("Error: the flush period must be an integer greater than or equal to 0");
			return;
		}

		try {
			next_timestamp_dur_sec = Double.parseDouble(line.getOptionValue("d", "" + next_timestamp_dur_sec));
		} catch (NumberFormatException nfe) {
			System.err.println("Error: the next timestamp duration must be a number greater than 0");
			return;
		}
		if (next_timestamp_dur_sec <= 0) {
			System.err.println("Error: the next timestamp duration must be a number greater than 0");
			return;
		}

		bPlasma = line.hasOption("plasma");

		bDebug = line.hasOption("debug");

		ctr = new CTreader("CTdata");

		// Setup Arrow-related variables
		PlasmaClient plasmaClient = null;
		if (bPlasma) {
			System.loadLibrary("plasma_java");
			plasmaClient = new PlasmaClient("/tmp/plasma", "", 0);
		}
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		ArrayList<Field> fields = new ArrayList<>();
		ArrayList<FieldVector> vectors = new ArrayList<>();
		// Create a separate container to hold CT timestamps
		ct_timestamp_dc = new DoubleDataContainer("ct_timestamp", "ct_timestamp", allocator);
		fields.add(ct_timestamp_dc.field);
		vectors.add(ct_timestamp_dc.fieldVec);
		for (int i = 0; i < arrow_chanNames.length; ++i) {
			switch (chanDataTypes[i]) {
				case INT_DATA:
					IntDataContainer int_dc = new IntDataContainer(arrow_chanNames[i], ct_chanNames[i], allocator);
					hashMap.put(arrow_chanNames[i], int_dc);
					break;
				case FLOAT_DATA:
					FloatDataContainer float_dc = new FloatDataContainer(arrow_chanNames[i], ct_chanNames[i], allocator);
					hashMap.put(arrow_chanNames[i], float_dc);
					break;
				case DOUBLE_DATA:
					DoubleDataContainer double_dc = new DoubleDataContainer(arrow_chanNames[i], ct_chanNames[i], allocator);
					hashMap.put(arrow_chanNames[i], double_dc);
					break;
				case STRING_DATA:
					StringDataContainer str_dc = new StringDataContainer(arrow_chanNames[i], ct_chanNames[i], allocator);
					hashMap.put(arrow_chanNames[i], str_dc);
					break;
			}
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			fields.add(dc.field);
			vectors.add(dc.fieldVec);
		}
		VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

		//
		// Fetch data from CloudTurbine source and write it to Arrow file or Plasma
		//
		// 1. Determine starting timestamp: Request oldest duration=0 data from either the first channel or
		//    (if it is being used) the trigger channel; the timestamp associated with this datapoint is our
		//    starting timestamp.
		// 2. Request data for all channels at this timestamp, duration=0
		// 3. Add received data to the data vectors in the DataContainer objects
		// 4. In a sleepy loop:
		//     a. Flush data if "flushPeriod_msec" has passed
		//     b. Check for updated data: Request data from either the first channel or (if it is being used) the
		//        trigger channel at time (latest_timestamp+0.0001) and large duration
		//     c. If we got updated data:
		//         i.  Save the new timestamp ("nextTimestamp")
		//         ii. If we are using a trigger channel and the value of the trigger channel has changed, flush data
		// 5. Go back to step 2
		//
		int recordsInBatch = 0;
		int triggerChanValue = -1 * Integer.MAX_VALUE;
		String timeRequestChanName = ct_chanNames[0];
		if (triggerChan != null) {
			timeRequestChanName = triggerChan;
		}
		CTdata oldestData = getDatapoint(timeRequestChanName, "oldest");
		double[] times = oldestData.getTime();
		double nextTimestamp = times[0];
		if (triggerChan != null) {
			// Save the starting value for the trigger channel
			int[] data = oldestData.getDataAsInt32();
			triggerChanValue = data[0];
		}
		long batchStartTime = System.currentTimeMillis();
		CTdata[] chanData = new CTdata[ct_chanNames.length];
		while (true) {
			System.err.println("Next CT timestamp = " + nextTimestamp);


			/*
			// Create request CTmap
			CTmap requestMap = new CTmap();
			for (int i = 0; i < ct_chanNames.length; ++i) {
				requestMap.add(ct_chanNames[i]);
			}
			// Update channel cache
			ctr.clearFileListCache();
			//
			// OPTION 1: absolute, zero-duration request; problem is that this can invoke the "at-or-before" logic
			// CTmap dataMap = ctr.getDataMap(requestMap, ct_sourceName, nextTimestamp, 0.0, "absolute");
			// if (dataMap == null) {
			// 	throw new Exception("ERROR: got null CTmap from our data request");
			// }
			//
			// Switch over to a non-zero duration to avoid at-or-before fetch logic
			// We've noticed occasional issues (when processing the weather data transmitted by Syncthing)
			// where the CTdata object for channels doesn't contain data for nextTimestamp but it does
			// contain data for an *earlier* timestamp.
			//
			// OPTION 2: Make a non-zero duration request (over a small interval around nextTimestamp) to avoid "at or before" data fetching.
			CTmap dataMap = ctr.getDataMap(requestMap, ct_sourceName, nextTimestamp - 0.0002, 0.0004, "absolute");
			// OPTION 3: To update cache on all channels, use a large duration
			// CTmap dataMap = ctr.getDataMap(requestMap, ct_sourceName, nextTimestamp-0.0002, next_timestamp_dur_sec, "after");
			// See if we got all channels in this dataMap
			for (int i = 0; i < ct_chanNames.length; ++i) {
				if (!dataMap.checkName(ct_chanNames[i])) {
					System.err.println("missing chan = " + ct_chanNames[i]);
				} else {
					CTdata ctData = dataMap.get(ct_chanNames[i]);
					if (ctData == null) {
						System.err.println("ctData is null for channel " + ct_chanNames[i]);
					} else {
						double[] timestamps = ctData.getTime();
						if (timestamps == null) {
							System.err.println("timestamps == null for chanel " + ct_chanNames[i]);
						} else if (timestamps.length == 0) {
							System.err.println("no timestamps for chanel " + ct_chanNames[i]);
						} else if (Math.abs(timestamps[0] - nextTimestamp) > 0.0001) {
							System.err.println("timestamp for chanel " + ct_chanNames[i] + " is off from nextTimestamp by " + (timestamps[0] - nextTimestamp));
						}
					}
				}
			}
			addDataToVectors(dataMap, recordsInBatch, nextTimestamp);
			*/


			for (int i = 0; i < ct_chanNames.length; ++i) {
				chanData[i] = ctr.getData(ct_sourceName, ct_chanNames[i], nextTimestamp - 0.0002, next_timestamp_dur_sec, "absolute");
			}
			addDataToVectors(chanData, recordsInBatch, nextTimestamp);



			++recordsInBatch;
			//
			// Do the following in a sleepy loop:
			// 1. Flush data if "flushPeriod_msec" has passed
			// 2. Check for updated data
			// 3. If we got updated data:
			//     a. Save the new timestamp ("nextTimestamp")
			//     b. If we are using a trigger channel and the value of the trigger channel has changed, then flush data
			//
			int loopCount = 0;
			while (true) {
				if (recordsInBatch > 0) {
					// We have some data to write to Arrow file or Plasma; see if the time has arrived to do that
					long currentTime = System.currentTimeMillis();
					if ((currentTime - batchStartTime) > flushPeriod_msec) {
						if (bDebug) {
							System.err.println("\nFlush period has expired  ==>  Flush data");
						}
						flushData(currentTime, plasmaClient, root, recordsInBatch);
						recordsInBatch = 0;
						batchStartTime = currentTime;
					}
				}
				// See if new data is available
				CTdata ctData = getNewData(timeRequestChanName, nextTimestamp);
				if (ctData != null) {
					times = ctData.getTime();
					nextTimestamp = times[0];
					if (triggerChan != null) {
						// See if the trigger channel value has changed
						int[] data = ctData.getDataAsInt32();
						if (Math.abs(triggerChanValue - data[0]) != 0) {
							// We got an updated trigger channel value; if there is data that has been stored, flush it
							triggerChanValue = data[0];
							if (bDebug) {
								System.err.print("\nNew value on trigger channel \"" + triggerChan + "\": " + triggerChanValue);
								if (recordsInBatch > 0) {
									System.err.println("  ==>  Flush data");
								} else {
									System.err.println(" ");
								}
							}
							if (recordsInBatch > 0) {
								long currentTime = System.currentTimeMillis();
								flushData(currentTime, plasmaClient, root, recordsInBatch);
								recordsInBatch = 0;
								batchStartTime = currentTime;
							}
						}
					}
					break;
				}
				++loopCount;
				if ( (loopCount % 30) == 0 ) {
					System.err.println("Waiting for next timestamp...");
				}
				Thread.sleep(100);
			}
		}
	}

	//
	// Get either the oldest or newest datapoint for the given channel.
	// Do this in a sleepy loop until we receive data.
	//
	// The given referenceI must either be "oldest" or "newest".
	//
	private CTdata getDatapoint(String chanNameI, String referenceI) throws Exception {
		if ( (!referenceI.equals("oldest")) && (!referenceI.equals("newest")) ) {
			throw new Exception("ERROR: getDatapoint(): referenceI must be oldest or newest");
		}
		int loopCtr = 0;
		while (true) {
			CTdata data = ctr.getData(ct_sourceName, chanNameI, 0., 0.0, referenceI);
			if ( (data != null) && (data.size() > 0) ) {
				// Since we made a duration=0 request, should only have 1 datapoint
				double[] dt = data.getTime();
				if (dt.length != 1) {
					throw new Exception("ERROR: getDatapoint(): length of time array = " + dt.length);
				}
				return data;
			}
			++loopCtr;
			if ( (loopCtr % 10) == 0) {
				System.err.println("Waiting for " + referenceI + " data");
			}
			Thread.sleep(100);
		}
	}

	//
	// Get the new data that comes *after* the given timestamp for the given channel.
	//
	private CTdata getNewData(String chanNameI, double timebaseI) throws Exception {
		CTdata data = ctr.getData(ct_sourceName, chanNameI, timebaseI+0.0001, next_timestamp_dur_sec, "absolute");
		if ( (data != null) && (data.size() > 0) ) {
			double[] dt = data.getTime();
			// Make sure time has advanced
			if (dt[0] > timebaseI) {
				return data;
			}
		}
		return null;
	}

	//
	// Add data from the given CTmap to the Arrow vectors
	//
	private void addDataToVectors(CTmap dataMapI, int indexI, double timestampI) {

		// Store CT timestamp
		ct_timestamp_dc.addDataToVector(indexI, timestampI);

		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			CTdata ctData = null;
			if (dataMapI.checkName(ct_chanNames[i])) {
				// dataMapI should already be trimmed to the desired time-range; no need to do the time request again
				// ctData = dataMapI.get(ct_chanNames[i], timestampI, 0.0, "absolute");
				ctData = dataMapI.get(ct_chanNames[i]);
			} else {
				System.err.println("WARNING: addDataToVectors(): No data for chan " + ct_chanNames[i] + "; add null");
			}
			dc.addDataToVector(ctData,indexI,timestampI);
		}
	}

	//
	// Add data from the given array of CTdata objects to the Arrow vectors
	//
	private void addDataToVectors(CTdata[] chanDataI, int indexI, double timestampI) {

		// Store CT timestamp
		ct_timestamp_dc.addDataToVector(indexI, timestampI);

		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			CTdata ctData = chanDataI[i];
			dc.addDataToVector(ctData,indexI,timestampI);
		}
	}

	//
	//
	//
	private void flushData(long currentTimeI, PlasmaClient plasmaClientI, VectorSchemaRoot rootI, int recordsInBatchI) {
		try {
			if (bPlasma) {
				System.err.println("FLUSH DATA TO PLASMA AT TIME " + currentTimeI);
				writeToPlasma(plasmaClientI, rootI, recordsInBatchI);
			} else {
				System.err.println("FLUSH DATA TO ARROW FILE AT TIME " + currentTimeI);
				writeToArrowFile(rootI, recordsInBatchI);
			}
		} catch (Exception e) {
			System.err.println("Caught exception writing data to Arrow:");
			System.err.println(e);
		}
		// Reset vectors
		ct_timestamp_dc.reset();
		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			dc.reset();
		}
	}

	//
	// Write data to an Arrow file
	// Each output file will contain one record batch
	//
	private void writeToArrowFile(VectorSchemaRoot rootI, int recordsInBatchI) throws FileNotFoundException,IOException {

		ct_timestamp_dc.setValueCount(recordsInBatchI);
		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			dc.setValueCount(recordsInBatchI);
		}

		++batchNum;

		// Create the filename
		String filename = String.format("%s_b%05d.arrow",ct_sourceName,batchNum);
		System.err.println("Batch " + batchNum + ", contains " + recordsInBatchI + " records; written to file " + filename);

		// This is a try-with-resource block
		try (FileOutputStream fos = new FileOutputStream(filename);
			 ArrowFileWriter fileWriter = new ArrowFileWriter(rootI, null, Channels.newChannel(fos)))
		{
			fileWriter.start();
			rootI.setRowCount(recordsInBatchI);
			fileWriter.writeBatch();
			fileWriter.end();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	} // end writeToArrowFile()

	//
	// Write data to Arrow and then Plasma
	// Each Plasma object will contain one record batch
	//
	private void writeToPlasma(PlasmaClient clientI, VectorSchemaRoot rootI, int recordsInBatchI) {

		ct_timestamp_dc.setValueCount(recordsInBatchI);
		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			dc.setValueCount(recordsInBatchI);
		}

		// This is a try-with-resource block
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			 ArrowStreamWriter writer = new ArrowStreamWriter(rootI, /*DictionaryProvider=*/null, Channels.newChannel(out)))
		{
			// Create the Arrow record batch in memory
			writer.start();

			++batchNum;

			// Create the Plasma object ID
			// See answer from "leo" at https://stackoverflow.com/questions/388461/how-can-i-pad-a-string-in-java
			String idStr = String.format("%-13s_b%05d",ct_sourceName,batchNum).replace(' ', '*');
			byte[] nextID = idStr.getBytes("UTF8");
			System.err.println("Batch " + batchNum + ", contains " + recordsInBatchI + " records; written to Plasma object " + idStr);

			rootI.setRowCount(recordsInBatchI);
			writer.writeBatch();
			writer.end();

			// Write the Arrow record batch to Plasma
			byte[] recordAsBytes = out.toByteArray();
			System.err.println("  - the record batch contains " + recordAsBytes.length + " bytes");
			// We could create a buffer in Plasma and then write into that buffer;
			// but the following call to client.put will do this
			// ByteBuffer plasmaBuf = clientI.create(nextID,recordAsBytes.length,null);
			clientI.put(nextID,recordAsBytes,null);
			// The client.put call above automatically seals the object in Plasma, don't do it again
			// client.seal(nextID);
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	} // end writeToPlasma()

} //end class CT2Arrow
