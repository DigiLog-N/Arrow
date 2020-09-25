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

CT2Plasma

Read data from a CT source (i.e. this program has a CT sink front-end) and
write it as record batches to a Plasma in-memory object store.

John Wilson, Erigo Technologies

This application is based on the code snippets found at https://arrow.apache.org/docs/java/ipc.html

Other useful URLs
Javadoc for Arrow API is found at https://arrow.apache.org/docs/java/reference/
Python Plasma API https://arrow.apache.org/docs/python/plasma.html
https://www.infoq.com/articles/apache-arrow-java/
https://github.com/animeshtrivedi/blog/blob/master/post/2017-12-26-arrow.md
https://github.com/animeshtrivedi/ArrowExample/blob/master/src/main/java/com/github/animeshtrivedi/arrowexample/ArrowWrite.java

 */

package erigo.ct2plasma;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;

import cycronix.ctlib.CTdata;
import cycronix.ctlib.CTmap;
import cycronix.ctlib.CTreader;
import org.apache.arrow.memory.*;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;

public class CT2Plasma {

	// Data types
	public enum DataType {INT_DATA, FLOAT_DATA, DOUBLE_DATA, STRING_DATA}

	// Map containing all of the DataContainer objects
	LinkedHashMap<String,DataContainer> hashMap = new LinkedHashMap<>();

	CTreader ctr = null;

	int batchFlushPeriod_msec = 120000; // Time (msec) between flushing data to Plasma

	// How many record batches we have written out?
	int batchNum = 0;

	// INPUT DATA THAT WILL CHANGE FROM SOURCE-TO-SOURCE
	// NB: It is assumed that this source resides below a "CTdata" folder
	// NB: Source name must be less than 13 characters
	String ct_sourceName = "PHM08";
	String[] ct_chanNames = {"unit.i32","time.i32","op1.f32","op2.f32","op3.f32","sensor01.f32","sensor02.f32","sensor03.f32","sensor04.f32","sensor05.f32","sensor06.f32","sensor07.f32","sensor08.f32","sensor09.f32","sensor10.f32","sensor11.f32","sensor12.f32","sensor13.f32","sensor14.f32","sensor15.f32","sensor16.f32","sensor17.f32","sensor18.f32","sensor19.f32","sensor20.f32","sensor21.f32"};
	String[] arrow_chanNames = {"unit","time","op1","op2","op3","sensor01","sensor02","sensor03","sensor04","sensor05","sensor06","sensor07","sensor08","sensor09","sensor10","sensor11","sensor12","sensor13","sensor14","sensor15","sensor16","sensor17","sensor18","sensor19","sensor20","sensor21"};
	DataType[] chanDataTypes = {DataType.INT_DATA,DataType.INT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA,DataType.FLOAT_DATA};

	//
	// Main function
	//
	public static void main(String[] argsI) {
		try {
			if (argsI.length < 1) {
				System.err.println("Usage:");
				System.err.println("    java -jar CT2Plasma.jar <CT source name>");
				System.exit(0);
			}
			new CT2Plasma(argsI[0]);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e);
		}
	}

	//
	// CT2Plasma constructor
	// Everything happens in this method
	//
	public CT2Plasma(String ctsourceI) throws Exception {

		// Check that the input arrays are all the same size
		if ( (ct_chanNames.length != arrow_chanNames.length) || (ct_chanNames.length != chanDataTypes.length) ) {
			throw new Exception("Input array size mismatch");
		}

		if (ct_sourceName.length() > 13) {
			throw new Exception("CT source name is too long; must be 13 characters at most");
		}

		ctr = new CTreader("CTdata");

		// Setup Arrow-related variables
		System.loadLibrary("plasma_java");
		PlasmaClient client = new PlasmaClient("/tmp/plasma", "", 0);
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		ArrayList<Field> fields = new ArrayList<>();
		ArrayList<FieldVector> vectors = new ArrayList<>();
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
		// Fetch data from CloudTurbine source and write it to Arrow + Plasma
		//
		// 1. Determine starting timestamp:
		//      a. Request oldest duration=0 data for the first channel
		//      b. Determine the timestamp of the received data; this is our starting timestamp
		// 2. Request data for all channels at this timestamp, duration=0
		// 3. Add received data to the data vectors
		// 4. If enough time has passed (based on batchFlushPeriod_msec), flush the data to Plasma
		// 5. Determine next timestamp:
		//      a. Request data for the first channel at (latest_timestamp+0.0001) and large duration
		//		b. Determine oldest timestamp in the received data; this is the next timestamp
		// 6. Go back to step 2
		//
		int recordsInBatch = 0;
		long batchStartTime = System.currentTimeMillis();
		double nextTimestamp = getOldestTimestamp(ct_chanNames[0]);
		while (true) {
			System.err.println("Next timestamp = " + nextTimestamp);
			// Create request CTmap
			CTmap requestMap = new CTmap();
			for (int i = 0; i < ct_chanNames.length; ++i) {
				requestMap.add(ct_chanNames[i]);
			}
			CTmap dataMap = ctr.getDataMap(requestMap, ct_sourceName, nextTimestamp, 0.0, "absolute");
			if (dataMap == null) {
				System.err.println("got null CTmap from our data request");
			} else {
				addDataToVectors(dataMap, recordsInBatch, nextTimestamp);
				++recordsInBatch;
				// Is it time to write data to Plasma? (We periodically flush data to Plasma.)
				if ((System.currentTimeMillis() - batchStartTime) > batchFlushPeriod_msec) {
					writeToPlasma(client, root, recordsInBatch);
					recordsInBatch = 0;
					// Reset vectors
					for (int i = 0; i < arrow_chanNames.length; ++i) {
						DataContainer dc = hashMap.get(arrow_chanNames[i]);
						dc.reset();
					}
					batchStartTime = System.currentTimeMillis();
				}
			}
			nextTimestamp = getNextTimestamp(ct_chanNames[0], nextTimestamp + 0.0001);
		}
	}

	//
	// Get the newest timestamp for the given channel.
	// Do this in a sleepy loop until we receive data.
	//
	private double getNewestTimestamp(String chanNameI) throws Exception {
		while (true) {
			CTdata data = ctr.getData(ct_sourceName, chanNameI, 0., 0.0, "newest");
			if (data != null) {
				// Should only be 1 timestamp
				double[] dt = data.getTime();
				if (dt.length != 1) {
					System.err.println("getNewestTimestamp(): length of time array = " + dt.length);
				}
				return dt[0];
			}
			Thread.sleep(100);
		}
	}

	//
	// Get the oldest timestamp for the given channel.
	// Do this in a sleepy loop until we receive data.
	//
	private double getOldestTimestamp(String chanNameI) throws Exception {
		while (true) {
			CTdata data = ctr.getData(ct_sourceName, chanNameI, 0., 0.0, "oldest");
			if (data != null) {
				// Should only be 1 timestamp
				double[] dt = data.getTime();
				if (dt.length != 1) {
					System.err.println("getOldestTimestamp(): length of time array = " + dt.length);
				}
				return dt[0];
			}
			Thread.sleep(100);
		}
	}

	//
	// Get the next timestamp for the given channel.
	// Do this in a sleepy loop until we receive data.
	// Perform absolute data request for the given channel at the given timestamp and large duration
	// Return the oldest timestamp from the received data
	//
	private double getNextTimestamp(String chanNameI, double timebaseI) throws Exception {
		while (true) {
			CTdata data = ctr.getData(ct_sourceName, chanNameI, timebaseI, 1000000, "absolute");
			if (data != null) {
				double[] dt = data.getTime();
				return dt[0];
			}
			Thread.sleep(100);
		}
	}

	//
	// Add data from the given CTmap to the Arrow vectors
	//
	private void addDataToVectors(CTmap dataMapI, int indexI, double timestampI) {
		for (int i = 0; i < arrow_chanNames.length; ++i) {
			DataContainer dc = hashMap.get(arrow_chanNames[i]);
			CTdata ctData = null;
			if (dataMapI.checkName(ct_chanNames[i])) {
				ctData = dataMapI.get(ct_chanNames[i], timestampI, 0.0, "absolute");
			} else {
				System.err.println("addDataToVectors(): No data for chan " + ct_chanNames[i] + "; add null");
			}
			dc.addDataToVector(ctData,indexI,timestampI);
		}
	}

	//
	// Write data to Arrow and then Plasma
	// Each Plasma object will contain one record batch
	//
	private void writeToPlasma(PlasmaClient clientI, VectorSchemaRoot rootI, int recordsInBatchI) {
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
			System.err.println("\nBatch " + batchNum + ", contains " + recordsInBatchI + " records");
			rootI.setRowCount(recordsInBatchI);
			writer.writeBatch();
			writer.end();

			// Create the Plasma object ID
			// See answer from "leo" at https://stackoverflow.com/questions/388461/how-can-i-pad-a-string-in-java
			String idStr = String.format("%-13s_b%05d",ct_sourceName,batchNum).replace(' ', '*');
			byte[] nextID = idStr.getBytes("UTF8");

			// Write the Arrow record batch to Plasma
			byte[] recordAsBytes = out.toByteArray();
			System.err.println("the record batch contains " + recordAsBytes.length + " bytes");
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

} //end class CT2Plasma
