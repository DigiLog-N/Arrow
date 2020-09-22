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

PHM08_to_Plasma

Reads data from a PHM08 input file and writes it out to Apache Plasma in-memory object store.

The PHM08 data is from the NASA jet engine prognostics challenge; from our shared repository, see Data/PHM08 or search for
"PHM08 Challenge Data Set" at https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/#turbofan
or go to https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/publications/#phm08_challenge

John Wilson, Erigo Technologies

This application is based on the code snippets found at https://arrow.apache.org/docs/java/ipc.html

Other useful URLs
Javadoc for Arrow API is found at https://arrow.apache.org/docs/java/reference/
Python Plasma API https://arrow.apache.org/docs/python/plasma.html
https://www.infoq.com/articles/apache-arrow-java/
https://github.com/animeshtrivedi/blog/blob/master/post/2017-12-26-arrow.md
https://github.com/animeshtrivedi/ArrowExample/blob/master/src/main/java/com/github/animeshtrivedi/arrowexample/ArrowWrite.java

 */

package erigo.phm08_to_plasma;

import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.*;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.*;

public class PHM08_to_Plasma {

	// Return codes for addDataToBatch
	static final int STR_ERROR = -1;     // Data wasn't added to the Vectors due to an error in the input string
	static final int UNIT_MISMATCH = -2; // Data wasn't added to the Vectors due to a mismatch in the unit number
	static final int DATA_SUCCESS = 1;   // Data was successfully added to the Vectors

	// Vectors to hold data to add to an Arrow batch
	// Should be one Vector here per channel
	IntVector unitVector = null;
	IntVector timeVector = null;
	Float4Vector op1Vector = null;
	Float4Vector op2Vector = null;
	Float4Vector op3Vector = null;
	Float4Vector sensor01Vector = null;
	Float4Vector sensor02Vector = null;
	Float4Vector sensor03Vector = null;
	Float4Vector sensor04Vector = null;
	Float4Vector sensor05Vector = null;
	Float4Vector sensor06Vector = null;
	Float4Vector sensor07Vector = null;
	Float4Vector sensor08Vector = null;
	Float4Vector sensor09Vector = null;
	Float4Vector sensor10Vector = null;
	Float4Vector sensor11Vector = null;
	Float4Vector sensor12Vector = null;
	Float4Vector sensor13Vector = null;
	Float4Vector sensor14Vector = null;
	Float4Vector sensor15Vector = null;
	Float4Vector sensor16Vector = null;
	Float4Vector sensor17Vector = null;
	Float4Vector sensor18Vector = null;
	Float4Vector sensor19Vector = null;
	Float4Vector sensor20Vector = null;
	Float4Vector sensor21Vector = null;

	//
	// Main function
	//
	public static void main(String[] argsI) {
		try {
			if (argsI.length < 1) {
				System.err.println("Usage:");
				System.err.println("    java -jar PHM08_to_Plasma.jar <in_filename>");
				System.exit(0);
			}
			new PHM08_to_Plasma(argsI[0]);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e);
		}
	}

	//
	// PHM08_to_Plasma constructor
	// Everything happens in this method
	//
	public PHM08_to_Plasma(String filenameI) throws Exception {

		// Next line from the data file
		String nextLine = null;

		File infile = new File(filenameI);
		if (!infile.isFile()) {
			System.err.println("The given file, " + filenameI + ", does not exist.");
			return;
		}
		// Open up the input file
		BufferedReader br = new BufferedReader(new FileReader(infile));

		// Create the Vectors to hold data; use the correct data type for each Vector
		// BitVector		(1 bit, elements can be null)
		// Float4Vector		(4 bytes, elements can be null)
		// Float8Vector		(8 bytes, elements can be null)
		// IntVector		(4 bytes, elements can be null)
		// BigIntVector		(8 bytes, elements can be null)
		// VarCharVector	(variable length vector, elements can be null)
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		unitVector = new IntVector("unit",allocator);
		timeVector = new IntVector("time_cycles",allocator);
		op1Vector = new Float4Vector("op1",allocator);
		op2Vector = new Float4Vector("op2",allocator);
		op3Vector = new Float4Vector("op3",allocator);
		sensor01Vector = new Float4Vector("sensor01",allocator);
		sensor02Vector = new Float4Vector("sensor02",allocator);
		sensor03Vector = new Float4Vector("sensor03",allocator);
		sensor04Vector = new Float4Vector("sensor04",allocator);
		sensor05Vector = new Float4Vector("sensor05",allocator);
		sensor06Vector = new Float4Vector("sensor06",allocator);
		sensor07Vector = new Float4Vector("sensor07",allocator);
		sensor08Vector = new Float4Vector("sensor08",allocator);
		sensor09Vector = new Float4Vector("sensor09",allocator);
		sensor10Vector = new Float4Vector("sensor10",allocator);
		sensor11Vector = new Float4Vector("sensor11",allocator);
		sensor12Vector = new Float4Vector("sensor12",allocator);
		sensor13Vector = new Float4Vector("sensor13",allocator);
		sensor14Vector = new Float4Vector("sensor14",allocator);
		sensor15Vector = new Float4Vector("sensor15",allocator);
		sensor16Vector = new Float4Vector("sensor16",allocator);
		sensor17Vector = new Float4Vector("sensor17",allocator);
		sensor18Vector = new Float4Vector("sensor18",allocator);
		sensor19Vector = new Float4Vector("sensor19",allocator);
		sensor20Vector = new Float4Vector("sensor20",allocator);
		sensor21Vector = new Float4Vector("sensor21",allocator);

		// Allocate space for the Vectors
		// We don't actually know ahead of time how many records there will be in each batch
		int batchSize = 100;
		unitVector.allocateNew(batchSize);
		timeVector.allocateNew(batchSize);
		op1Vector.allocateNew(batchSize);
		op2Vector.allocateNew(batchSize);
		op3Vector.allocateNew(batchSize);
		sensor01Vector.allocateNew(batchSize);
		sensor02Vector.allocateNew(batchSize);
		sensor03Vector.allocateNew(batchSize);
		sensor04Vector.allocateNew(batchSize);
		sensor05Vector.allocateNew(batchSize);
		sensor06Vector.allocateNew(batchSize);
		sensor07Vector.allocateNew(batchSize);
		sensor08Vector.allocateNew(batchSize);
		sensor09Vector.allocateNew(batchSize);
		sensor10Vector.allocateNew(batchSize);
		sensor11Vector.allocateNew(batchSize);
		sensor12Vector.allocateNew(batchSize);
		sensor13Vector.allocateNew(batchSize);
		sensor14Vector.allocateNew(batchSize);
		sensor15Vector.allocateNew(batchSize);
		sensor16Vector.allocateNew(batchSize);
		sensor17Vector.allocateNew(batchSize);
		sensor18Vector.allocateNew(batchSize);
		sensor19Vector.allocateNew(batchSize);
		sensor20Vector.allocateNew(batchSize);
		sensor21Vector.allocateNew(batchSize);

		// Write data to the first batch
		int recordsInBatch = 0;
		int currentUnitNumber = 1;
		while (true) {
			nextLine = br.readLine();
			if (nextLine == null) {
				System.err.println("Got null reading file when making first batch");
				return;
			}
			int returnVal = addDataToBatch(recordsInBatch, currentUnitNumber, nextLine);
			if (returnVal == DATA_SUCCESS) {
				++recordsInBatch;
			} else if (returnVal == UNIT_MISMATCH) {
				// We've reached the next unit number
				break;
			}
		}

		// Specify size for each Vector
		unitVector.setValueCount(recordsInBatch);
		timeVector.setValueCount(recordsInBatch);
		op1Vector.setValueCount(recordsInBatch);
		op2Vector.setValueCount(recordsInBatch);
		op3Vector.setValueCount(recordsInBatch);
		sensor01Vector.setValueCount(recordsInBatch);
		sensor02Vector.setValueCount(recordsInBatch);
		sensor03Vector.setValueCount(recordsInBatch);
		sensor04Vector.setValueCount(recordsInBatch);
		sensor05Vector.setValueCount(recordsInBatch);
		sensor06Vector.setValueCount(recordsInBatch);
		sensor07Vector.setValueCount(recordsInBatch);
		sensor08Vector.setValueCount(recordsInBatch);
		sensor09Vector.setValueCount(recordsInBatch);
		sensor10Vector.setValueCount(recordsInBatch);
		sensor11Vector.setValueCount(recordsInBatch);
		sensor12Vector.setValueCount(recordsInBatch);
		sensor13Vector.setValueCount(recordsInBatch);
		sensor14Vector.setValueCount(recordsInBatch);
		sensor15Vector.setValueCount(recordsInBatch);
		sensor16Vector.setValueCount(recordsInBatch);
		sensor17Vector.setValueCount(recordsInBatch);
		sensor18Vector.setValueCount(recordsInBatch);
		sensor19Vector.setValueCount(recordsInBatch);
		sensor20Vector.setValueCount(recordsInBatch);
		sensor21Vector.setValueCount(recordsInBatch);

		List<Field> fields = Arrays.asList(
				unitVector.getField(),
				timeVector.getField(),
				op1Vector.getField(),
				op2Vector.getField(),
				op3Vector.getField(),
				sensor01Vector.getField(),
				sensor02Vector.getField(),
				sensor03Vector.getField(),
				sensor04Vector.getField(),
				sensor05Vector.getField(),
				sensor06Vector.getField(),
				sensor07Vector.getField(),
				sensor08Vector.getField(),
				sensor09Vector.getField(),
				sensor10Vector.getField(),
				sensor11Vector.getField(),
				sensor12Vector.getField(),
				sensor13Vector.getField(),
				sensor14Vector.getField(),
				sensor15Vector.getField(),
				sensor16Vector.getField(),
				sensor17Vector.getField(),
				sensor18Vector.getField(),
				sensor19Vector.getField(),
				sensor20Vector.getField(),
				sensor21Vector.getField());
		List<FieldVector> vectors = Arrays.asList(
				unitVector,
				timeVector,
				op1Vector,
				op2Vector,
				op3Vector,
				sensor01Vector,
				sensor02Vector,
				sensor03Vector,
				sensor04Vector,
				sensor05Vector,
				sensor06Vector,
				sensor07Vector,
				sensor08Vector,
				sensor09Vector,
				sensor10Vector,
				sensor11Vector,
				sensor12Vector,
				sensor13Vector,
				sensor14Vector,
				sensor15Vector,
				sensor16Vector,
				sensor17Vector,
				sensor18Vector,
				sensor19Vector,
				sensor20Vector,
				sensor21Vector);

		VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

		// Keep track of how many batches we have written out
		int batchNum = 0;

		// This is a try-with-resource block
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			 ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out)))
		{
			writer.start();
			// Write out first batch of data
			++batchNum;
			System.err.println("\nBatch " + batchNum + ", contains " + recordsInBatch + " records");
			root.setRowCount(recordsInBatch);
			writer.writeBatch();
			// Continuously write new batches until we reach end of file
			boolean bEOF = false;
			while (true) {
				// Reset vectors
				unitVector.reset();
				timeVector.reset();
				op1Vector.reset();
				op2Vector.reset();
				op3Vector.reset();
				sensor01Vector.reset();
				sensor02Vector.reset();
				sensor03Vector.reset();
				sensor04Vector.reset();
				sensor05Vector.reset();
				sensor06Vector.reset();
				sensor07Vector.reset();
				sensor08Vector.reset();
				sensor09Vector.reset();
				sensor10Vector.reset();
				sensor11Vector.reset();
				sensor12Vector.reset();
				sensor13Vector.reset();
				sensor14Vector.reset();
				sensor15Vector.reset();
				sensor16Vector.reset();
				sensor17Vector.reset();
				sensor18Vector.reset();
				sensor19Vector.reset();
				sensor20Vector.reset();
				sensor21Vector.reset();
				// Write out a new batch
				++currentUnitNumber;
				recordsInBatch = 0;
				boolean bFirstLine = true;
				while (true) {
					if (bFirstLine) {
						// We'll use the last line we read in (has the new unit number)
						bFirstLine = false;
					} else {
						nextLine = br.readLine();
					}
					if (nextLine == null) {
						System.err.println("We've reached the end of the file");
						bEOF = true;
						break;
					}
					int returnVal = addDataToBatch(recordsInBatch, currentUnitNumber, nextLine);
					if (returnVal == DATA_SUCCESS) {
						++recordsInBatch;
					} else if (returnVal == UNIT_MISMATCH) {
						// We've reached the next unit number
						break;
					}
				}
				// Write out next batch of data
				if ( recordsInBatch > 0 ) {
					++batchNum;
					System.err.println("Batch " + batchNum + ", contains " + recordsInBatch + " records");
					root.setRowCount(recordsInBatch);
					writer.writeBatch();
				}
				if (bEOF) {
					// Write the batch out to Plasma
					System.loadLibrary("plasma_java");
					PlasmaClient client = new PlasmaClient("/tmp/plasma", "", 0);
					byte[] nextID = new byte[20];
					Arrays.fill(nextID, (byte) 1);
					byte[] recordAsBytes = out.toByteArray();
					System.err.println("the record batch contains " + recordAsBytes.length + " bytes");
					// We could create a buffer in Plasma and then write into that buffer;
					// but the following call to client.put will do this
					// ByteBuffer plasmaBuf = client.create(nextID,recordAsBytes.length,null);
					client.put(nextID,recordAsBytes,null);
					// The client.put call above automatically seals the object in Plasma, don't do it again
					// client.seal(nextID);
					break;
				}
			}
			// Close the ArrowFileWriter
			writer.end();
			br.close();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	}

	//
	// Break up a given CSV string and add one datapoint to each Vector
	//
	// Return value:
	// STR_ERROR:     Data wasn't added to the Vectors due to an error in the input string
	// UNIT_MISMATCH: Data wasn't added to the Vectors due to a mismatch in the unit number
	// DATA_SUCCESS:  Data was successfully added to the Vectors
	//
	private int addDataToBatch(int indexI, int currentUnitNumberI, String csvStrI) {
		if (csvStrI == null) {
			return STR_ERROR;
		}
		String csvStr = csvStrI.trim();
		if (csvStr.isEmpty()) {
			return STR_ERROR;
		}
		String[] strArray = csvStr.split(" ");
		if (strArray.length != 26) {
			System.err.println("addDataToBatch(): got wrong number of array entries: expected 26, got " + strArray.length);
			return STR_ERROR;
		}
		//
		// Make sure the unit number in the parsed string is the same as the given unit number
		//
		String unitNumberStr = strArray[0];
		if ( (unitNumberStr == null) || (unitNumberStr.trim().isEmpty()) ) {
			return STR_ERROR;
		}
		int unitNumber = -1;
		unitNumberStr = unitNumberStr.trim();
		try {
			unitNumber = Integer.parseInt(unitNumberStr);
		} catch (Exception e) {
			return STR_ERROR;
		}
		if (unitNumber != currentUnitNumberI) {
			return UNIT_MISMATCH;
		}
		//
		// Add data to the Vectors
		//
		storeAsInteger(strArray[0],unitVector,indexI);
		storeAsInteger(strArray[1],timeVector,indexI);
		storeAsFloat(strArray[2],op1Vector,indexI);
		storeAsFloat(strArray[3],op2Vector,indexI);
		storeAsFloat(strArray[4],op3Vector,indexI);
		storeAsFloat(strArray[5],sensor01Vector,indexI);
		storeAsFloat(strArray[6],sensor02Vector,indexI);
		storeAsFloat(strArray[7],sensor03Vector,indexI);
		storeAsFloat(strArray[8],sensor04Vector,indexI);
		storeAsFloat(strArray[9],sensor05Vector,indexI);
		storeAsFloat(strArray[10],sensor06Vector,indexI);
		storeAsFloat(strArray[11],sensor07Vector,indexI);
		storeAsFloat(strArray[12],sensor08Vector,indexI);
		storeAsFloat(strArray[13],sensor09Vector,indexI);
		storeAsFloat(strArray[14],sensor10Vector,indexI);
		storeAsFloat(strArray[15],sensor11Vector,indexI);
		storeAsFloat(strArray[16],sensor12Vector,indexI);
		storeAsFloat(strArray[17],sensor13Vector,indexI);
		storeAsFloat(strArray[18],sensor14Vector,indexI);
		storeAsFloat(strArray[19],sensor15Vector,indexI);
		storeAsFloat(strArray[20],sensor16Vector,indexI);
		storeAsFloat(strArray[21],sensor17Vector,indexI);
		storeAsFloat(strArray[22],sensor18Vector,indexI);
		storeAsFloat(strArray[23],sensor19Vector,indexI);
		storeAsFloat(strArray[24],sensor20Vector,indexI);
		storeAsFloat(strArray[25],sensor21Vector,indexI);
		return DATA_SUCCESS;
	}

	//
	// Add a "bit" to a BitVector at the given index
	// Very similar to storeAsInteger, but the only acceptable values are 0 or 1 or null
	//
	private void storeAsBit(String strI,BitVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// Here's another way to do it
			// vecI.setNull(indexI);
			vecI.setSafe(indexI, 0, 0);
			return;
		}
		Integer intVal = null;
		String str = strI.trim();
		try {
			intVal = Integer.parseInt(str);
		} catch (Exception e) {
			intVal = null;
		}
		if ( (intVal == null) || ( (intVal != 0) && (intVal != 1) ) ) {
			vecI.setSafe(indexI, 0, 0);
		} else {
			vecI.setSafe(indexI, intVal);
		}
	}

	//
	// Add int to an IntVector at the given index
	//
	private void storeAsInteger(String strI,IntVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// Here's another way to do it
			// vecI.setNull(indexI);
			vecI.setSafe(indexI, 0, -999);
			return;
		}
		Integer intVal = null;
		String str = strI.trim();
		try {
			intVal = Integer.parseInt(str);
		} catch (Exception e) {
			intVal = null;
		}
		if (intVal == null) {
			vecI.setSafe(indexI, 0, -999);
		} else {
			vecI.setSafe(indexI, intVal);
		}
	}

	//
	// Add long to a BigIntVector at the given index
	//
	private void storeAsLong(String strI,BigIntVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// Here's another way to do it
			// vecI.setNull(indexI);
			vecI.setSafe(indexI, 0, -999);
			return;
		}
		Long longVal = null;
		String str = strI.trim();
		try {
			longVal = Long.parseLong(str);
		} catch (Exception e) {
			longVal = null;
		}
		if (longVal == null) {
			vecI.setSafe(indexI, 0, -999);
		} else {
			vecI.setSafe(indexI, longVal);
		}
	}

	//
	// Add float to a Float4Vector at the given index
	//
	private void storeAsFloat(String strI,Float4Vector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// Here's another way to do it
			// vecI.setNull(indexI);
			vecI.setSafe(indexI, 0, -999);
			return;
		}
		Float floatVal = null;
		String str = strI.trim();
		try {
			floatVal = Float.parseFloat(str);
		} catch (Exception e) {
			floatVal = null;
		}
		if (floatVal == null) {
			vecI.setSafe(indexI, 0, -999);
		} else {
			vecI.setSafe(indexI, floatVal);
		}
	}

	//
	// Add string data (as bytes) to a VarCharVector at the given index
	//
	private void storeAsStrBytes(String strI,VarCharVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// NB: There's no "setSafe" function which includes the "isSet" argument for a VarCharVector
			// Can either risk it and call the "setNull" function (which won't be good if our index is over the limit
			// or we can just store a "n/a" string
			// vecI.setNull(indexI);
			vecI.setSafe(indexI, "n/a".getBytes(StandardCharsets.UTF_8));
		} else {
			vecI.setSafe(indexI, strI.trim().getBytes(StandardCharsets.UTF_8));
		}
	}
	
} //end class PHM08_to_Plasma
