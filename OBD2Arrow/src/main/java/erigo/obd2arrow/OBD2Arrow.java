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

OBD2Arrow

Reads OBD data from an input file and write it out as Arrow record batches to an Arrow file.

Sample input file: Data/OBD/v2/dailyRoutes.csv
(I think this is a somewhat cleaned-up version of "exp1_14drivers_14cars_dailyRoutes.csv" from https://www.kaggle.com/cephasax/obdii-ds3?select=exp1_14drivers_14cars_dailyRoutes.csv)

For the above sample input file, we have the corresponding output file at Data/OBD/v2/dailyRoutes.arrow
(there are 100 data rows (records) per batch except for the last batch (which contains 28 rows); total of 951 batches)

John Wilson, Erigo Technologies

This application is based on the code snippets found at https://arrow.apache.org/docs/java/ipc.html

Other useful URLs
Javadoc for Arrow API is found at https://arrow.apache.org/docs/java/reference/
https://www.infoq.com/articles/apache-arrow-java/
https://github.com/animeshtrivedi/blog/blob/master/post/2017-12-26-arrow.md
https://github.com/animeshtrivedi/ArrowExample/blob/master/src/main/java/com/github/animeshtrivedi/arrowexample/ArrowWrite.java

 */

package erigo.obd2arrow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.*;

public class OBD2Arrow {

	// Vectors to hold data to add to an Arrow batch
	// Should be one Vector here per channel
	BigIntVector timestampVector = null;
	VarCharVector markVector = null;
	VarCharVector modelVector = null;
	IntVector carYearVector = null;
	Float4Vector enginePowerVector = null;
	VarCharVector automaticVector = null;
	VarCharVector vehicleIdVector = null;
	Float4Vector barometricPressureKpaVector = null;
	Float4Vector engineCoolantTempVector = null;
	Float4Vector fuelLevelVector = null;
	Float4Vector engineLoadVector = null;
	Float4Vector ambientAirTempVector = null;
	Float4Vector engineRpmVector = null;
	Float4Vector intakeManifoldPressureVector = null;
	Float4Vector mafVector = null;
	Float4Vector longTermFuelTrimBank2Vector = null;
	VarCharVector fuelTypeVector = null;
	Float4Vector airIntakeTempVector = null;
	Float4Vector fuelPressureVector = null;
	Float4Vector speedVector = null;
	Float4Vector shortTermFuelTrimBank2Vector = null;
	Float4Vector shortTermFuelTrimBank1Vector = null;
	VarCharVector engineRuntimeVector = null;
	Float4Vector throttlePosVector = null;
	VarCharVector dtcNumberVector = null;
	VarCharVector troubleCodesVector = null;
	Float4Vector timingAdvanceVector = null;
	Float4Vector equivRatioVector = null;
	IntVector minVector = null;
	IntVector hoursVector = null;
	IntVector daysOfWeekVector = null;
	IntVector monthsVector = null;
	IntVector yearVector = null;
	BitVector classVector = null;

	// Desired number of records per batch
	public static final int batchSize = 100;

	// Names of the input and output files
	String inFilename = ".\\dailyRoutes.csv";
	String outFilename = ".\\dailyRoutes.arrow";

	//
	// Main function
	//
	public static void main(String[] arg) {
		try {
			new OBD2Arrow(arg);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e);
		}
	}

	//
	// OBD2Arrow constructor
	// Everything happens in this method
	//
	public OBD2Arrow(String[] arg) throws Exception {

		// Open up the input CSV file
		BufferedReader br = new BufferedReader(new FileReader(inFilename));
		// Skip the first line in the input file (it contains column headings)
		String csvStr = br.readLine();

		// Create the Vectors to hold data; use the correct data type for each Vector
		// BitVector		(1 bit, elements can be null)
		// Float4Vector		(4 bytes, elements can be null)
		// Float8Vector		(8 bytes, elements can be null)
		// IntVector		(4 bytes, elements can be null)
		// BigIntVector		(8 bytes, elements can be null)
		// VarCharVector	(variable length vector, elements can be null)
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		timestampVector = new BigIntVector("TIMESTAMP",allocator);
		markVector = new VarCharVector("MARK", allocator);
		modelVector = new VarCharVector("MODEL", allocator);
		carYearVector = new IntVector("CAR_YEAR",allocator);
		enginePowerVector = new Float4Vector("ENGINE_POWER",allocator);
		automaticVector = new VarCharVector("AUTOMATIC", allocator);
		vehicleIdVector = new VarCharVector("VEHICLE_ID", allocator);
		barometricPressureKpaVector = new Float4Vector("BAROMETRIC_PRESSURE_KPA",allocator);
		engineCoolantTempVector = new Float4Vector("ENGINE_COOLANT_TEMP",allocator);
		fuelLevelVector = new Float4Vector("FUEL_LEVEL",allocator);
		engineLoadVector = new Float4Vector("ENGINE_LOAD",allocator);
		ambientAirTempVector = new Float4Vector("AMBIENT_AIR_TEMP",allocator);
		engineRpmVector = new Float4Vector("ENGINE_RPM",allocator);
		intakeManifoldPressureVector = new Float4Vector("INTAKE_MANIFOLD_PRESSURE",allocator);
		mafVector = new Float4Vector("MAF",allocator);
		longTermFuelTrimBank2Vector = new Float4Vector("LONG_TERM_FUEL_TRIM_BANK_2",allocator);
		fuelTypeVector = new VarCharVector("FUEL_TYPE", allocator);
		airIntakeTempVector = new Float4Vector("AIR_INTAKE_TEMP",allocator);
		fuelPressureVector = new Float4Vector("FUEL_PRESSURE",allocator);
		speedVector = new Float4Vector("SPEED",allocator);
		shortTermFuelTrimBank2Vector = new Float4Vector("SHORT_TERM_FUEL_TRIM_BANK_2",allocator);
		shortTermFuelTrimBank1Vector = new Float4Vector("SHORT_TERM_FUEL_TRIM_BANK_1",allocator);
		engineRuntimeVector = new VarCharVector("ENGINE_RUNTIME", allocator);
		throttlePosVector = new Float4Vector("THROTTLE_POS",allocator);
		dtcNumberVector = new VarCharVector("DTC_NUMBER", allocator);
		troubleCodesVector = new VarCharVector("TROUBLE_CODES", allocator);
		timingAdvanceVector = new Float4Vector("TIMING_ADVANCE",allocator);
		equivRatioVector = new Float4Vector("EQUIV_RATIO",allocator);
		minVector = new IntVector("MIN",allocator);
		hoursVector = new IntVector("HOURS",allocator);
		daysOfWeekVector = new IntVector("DAYS_OF_WEEK",allocator);
		monthsVector = new IntVector("MONTHS",allocator);
		yearVector = new IntVector("YEAR",allocator);
		classVector = new BitVector("CLASS", allocator);

		// Allocate space for the Vectors
		timestampVector.allocateNew(batchSize);
		markVector.allocateNew(batchSize);
		modelVector.allocateNew(batchSize);
		carYearVector.allocateNew(batchSize);
		enginePowerVector.allocateNew(batchSize);
		automaticVector.allocateNew(batchSize);
		vehicleIdVector.allocateNew(batchSize);
		barometricPressureKpaVector.allocateNew(batchSize);
		engineCoolantTempVector.allocateNew(batchSize);
		fuelLevelVector.allocateNew(batchSize);
		engineLoadVector.allocateNew(batchSize);
		ambientAirTempVector.allocateNew(batchSize);
		engineRpmVector.allocateNew(batchSize);
		intakeManifoldPressureVector.allocateNew(batchSize);
		mafVector.allocateNew(batchSize);
		longTermFuelTrimBank2Vector.allocateNew(batchSize);
		fuelTypeVector.allocateNew(batchSize);
		airIntakeTempVector.allocateNew(batchSize);
		fuelPressureVector.allocateNew(batchSize);
		speedVector.allocateNew(batchSize);
		shortTermFuelTrimBank2Vector.allocateNew(batchSize);
		shortTermFuelTrimBank1Vector.allocateNew(batchSize);
		engineRuntimeVector.allocateNew(batchSize);
		throttlePosVector.allocateNew(batchSize);
		dtcNumberVector.allocateNew(batchSize);
		troubleCodesVector.allocateNew(batchSize);
		timingAdvanceVector.allocateNew(batchSize);
		equivRatioVector.allocateNew(batchSize);
		minVector.allocateNew(batchSize);
		hoursVector.allocateNew(batchSize);
		daysOfWeekVector.allocateNew(batchSize);
		monthsVector.allocateNew(batchSize);
		yearVector.allocateNew(batchSize);
		classVector.allocateNew(batchSize);

		// Write first record
		int arrowFileIndex = 0;
		while (true) {
			csvStr = br.readLine();
			if (csvStr == null) {
				System.err.println("Got null reading file when making first batch");
				return;
			}
			boolean bSuccess = addDataToBatch(arrowFileIndex, csvStr);
			if (bSuccess) {
				++arrowFileIndex;
				if (arrowFileIndex >= batchSize) {
					break;
				}
			}
		}

		// Specify size for each Vector
		timestampVector.setValueCount(batchSize);
		markVector.setValueCount(batchSize);
		modelVector.setValueCount(batchSize);
		carYearVector.setValueCount(batchSize);
		enginePowerVector.setValueCount(batchSize);
		automaticVector.setValueCount(batchSize);
		vehicleIdVector.setValueCount(batchSize);
		barometricPressureKpaVector.setValueCount(batchSize);
		engineCoolantTempVector.setValueCount(batchSize);
		fuelLevelVector.setValueCount(batchSize);
		engineLoadVector.setValueCount(batchSize);
		ambientAirTempVector.setValueCount(batchSize);
		engineRpmVector.setValueCount(batchSize);
		intakeManifoldPressureVector.setValueCount(batchSize);
		mafVector.setValueCount(batchSize);
		longTermFuelTrimBank2Vector.setValueCount(batchSize);
		fuelTypeVector.setValueCount(batchSize);
		airIntakeTempVector.setValueCount(batchSize);
		fuelPressureVector.setValueCount(batchSize);
		speedVector.setValueCount(batchSize);
		shortTermFuelTrimBank2Vector.setValueCount(batchSize);
		shortTermFuelTrimBank1Vector.setValueCount(batchSize);
		engineRuntimeVector.setValueCount(batchSize);
		throttlePosVector.setValueCount(batchSize);
		dtcNumberVector.setValueCount(batchSize);
		troubleCodesVector.setValueCount(batchSize);
		timingAdvanceVector.setValueCount(batchSize);
		equivRatioVector.setValueCount(batchSize);
		minVector.setValueCount(batchSize);
		hoursVector.setValueCount(batchSize);
		daysOfWeekVector.setValueCount(batchSize);
		monthsVector.setValueCount(batchSize);
		yearVector.setValueCount(batchSize);
		classVector.setValueCount(batchSize);
		
		List<Field> fields = Arrays.asList(
				timestampVector.getField(),
				markVector.getField(),
				modelVector.getField(),
				carYearVector.getField(),
				enginePowerVector.getField(),
				automaticVector.getField(),
				vehicleIdVector.getField(),
				barometricPressureKpaVector.getField(),
				engineCoolantTempVector.getField(),
				fuelLevelVector.getField(),
				engineLoadVector.getField(),
				ambientAirTempVector.getField(),
				engineRpmVector.getField(),
				intakeManifoldPressureVector.getField(),
				mafVector.getField(),
				longTermFuelTrimBank2Vector.getField(),
				fuelTypeVector.getField(),
				airIntakeTempVector.getField(),
				fuelPressureVector.getField(),
				speedVector.getField(),
				shortTermFuelTrimBank2Vector.getField(),
				shortTermFuelTrimBank1Vector.getField(),
				engineRuntimeVector.getField(),
				throttlePosVector.getField(),
				dtcNumberVector.getField(),
				troubleCodesVector.getField(),
				timingAdvanceVector.getField(),
				equivRatioVector.getField(),
				minVector.getField(),
				hoursVector.getField(),
				daysOfWeekVector.getField(),
				monthsVector.getField(),
				yearVector.getField(),
				classVector.getField());
		List<FieldVector> vectors = Arrays.asList(
				timestampVector,
				markVector,
				modelVector,
				carYearVector,
				enginePowerVector,
				automaticVector,
				vehicleIdVector,
				barometricPressureKpaVector,
				engineCoolantTempVector,
				fuelLevelVector,
				engineLoadVector,
				ambientAirTempVector,
				engineRpmVector,
				intakeManifoldPressureVector,
				mafVector,
				longTermFuelTrimBank2Vector,
				fuelTypeVector,
				airIntakeTempVector,
				fuelPressureVector,
				speedVector,
				shortTermFuelTrimBank2Vector,
				shortTermFuelTrimBank1Vector,
				engineRuntimeVector,
				throttlePosVector,
				dtcNumberVector,
				troubleCodesVector,
				timingAdvanceVector,
				equivRatioVector,
				minVector,
				hoursVector,
				daysOfWeekVector,
				monthsVector,
				yearVector,
				classVector);

		VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

		// Keep track of how many batches we have written out
		int batchNum = 0;

		// "null" csv string, used if we want to pad a not-full batch with null data
		String nullCsvStr = "a, , , , , , , , , , , , , , , , , , , , , , , , , , , , , , , , ,a";

		// This is a try-with-resource block
		try (FileOutputStream fos = new FileOutputStream(outFilename);
			// Make the Arrow writer
            ArrowFileWriter arrowFileWriter = new ArrowFileWriter(root, null, Channels.newChannel(fos))) {
			arrowFileWriter.start();
			// Write out first batch of data
			++batchNum;
			System.err.println("\nBatch " + batchNum + ", contains " + batchSize + " records");
			root.setRowCount(batchSize);
			arrowFileWriter.writeBatch();
			// Continuously write new batches until we reach end of file
			boolean bEOF = false;
			while (true) {
				// Reset vectors
				timestampVector.reset();
				markVector.reset();
				modelVector.reset();
				carYearVector.reset();
				enginePowerVector.reset();
				automaticVector.reset();
				vehicleIdVector.reset();
				barometricPressureKpaVector.reset();
				engineCoolantTempVector.reset();
				fuelLevelVector.reset();
				engineLoadVector.reset();
				ambientAirTempVector.reset();
				engineRpmVector.reset();
				intakeManifoldPressureVector.reset();
				mafVector.reset();
				longTermFuelTrimBank2Vector.reset();
				fuelTypeVector.reset();
				airIntakeTempVector.reset();
				fuelPressureVector.reset();
				speedVector.reset();
				shortTermFuelTrimBank2Vector.reset();
				shortTermFuelTrimBank1Vector.reset();
				engineRuntimeVector.reset();
				throttlePosVector.reset();
				dtcNumberVector.reset();
				troubleCodesVector.reset();
				timingAdvanceVector.reset();
				equivRatioVector.reset();
				minVector.reset();
				hoursVector.reset();
				daysOfWeekVector.reset();
				monthsVector.reset();
				yearVector.reset();
				classVector.reset();
				// Write out a new batch
				arrowFileIndex = 0;
				while (true) {
					csvStr = br.readLine();
					if (csvStr == null) {
						System.err.println("We've reached the end of the file");
						bEOF = true;
						break;
					}
					boolean bSuccess = addDataToBatch(arrowFileIndex, csvStr);
					if (bSuccess) {
						++arrowFileIndex;
						if (arrowFileIndex >= batchSize) {
							break;
						}
					}
				}
				/**
				 * If we want to have every batch the same size, use the following code
				if ( (bEOF) && (arrowFileIndex != 0) && (arrowFileIndex != batchSize) ) {
					// We reached end of file but the batch isn't full
					// Pad the remainder of this record with null data (use nullCsvStr)
					int num_addl = batchSize - arrowFileIndex;
					System.err.println("At end of file, but batch isn't full: it currently contains " + arrowFileIndex + " records, need " + num_addl + " more.");
					for (int i = 0; i < num_addl; ++i) {
						addDataToBatch(arrowFileIndex, nullCsvStr);
					}
				}
				**/
				// Write out next batch of data
				if ( arrowFileIndex > 0 ) {
					++batchNum;
					if ( ((batchNum % 10) == 0) || (arrowFileIndex != batchSize) || (bEOF) ) {
						System.err.println("Batch " + batchNum + ", contains " + arrowFileIndex + " records");
					}
					root.setRowCount(arrowFileIndex);
					arrowFileWriter.writeBatch();
				}
				if (bEOF) {
					break;
				}
			}
			// Close the ArrowFileWriter
			arrowFileWriter.end();
			br.close();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	}

	//
	// Break up a given CSV string and add one datapoint to each Vector
	//
	private boolean addDataToBatch(int arrowFileIndexI, String csvStrI) {
		if (arrowFileIndexI > (batchSize-1)) {
			System.err.println("addDataToBatch(): ERROR: the given index (" + arrowFileIndexI + ") is greater than " + (batchSize-1));
			return false;
		}
		if (csvStrI == null) {
			return false;
		}
		String csvStr = csvStrI.trim();
		if (csvStr.isEmpty()) {
			return false;
		}
		String[] strArray = csvStr.split(",");
		if (strArray.length != 34) {
			System.err.println("addDataToBatch(): got wrong number of array entries: expected 34, got " + strArray.length);
			return false;
		}
		storeAsLong(strArray[0],timestampVector,arrowFileIndexI);
		storeAsStrBytes(strArray[1],markVector,arrowFileIndexI);
		storeAsStrBytes(strArray[2],modelVector,arrowFileIndexI);
		storeAsInteger(strArray[3],carYearVector,arrowFileIndexI);
		storeAsFloat(strArray[4],enginePowerVector,arrowFileIndexI);
		storeAsStrBytes(strArray[5],automaticVector,arrowFileIndexI);
		storeAsStrBytes(strArray[6],vehicleIdVector,arrowFileIndexI);
		storeAsFloat(strArray[7],barometricPressureKpaVector,arrowFileIndexI);
		storeAsFloat(strArray[8],engineCoolantTempVector,arrowFileIndexI);
		storeAsFloat(strArray[9],fuelLevelVector,arrowFileIndexI);
		storeAsFloat(strArray[10],engineLoadVector,arrowFileIndexI);
		storeAsFloat(strArray[11],ambientAirTempVector,arrowFileIndexI);
		storeAsFloat(strArray[12],engineRpmVector,arrowFileIndexI);
		storeAsFloat(strArray[13],intakeManifoldPressureVector,arrowFileIndexI);
		storeAsFloat(strArray[14],mafVector,arrowFileIndexI);
		storeAsFloat(strArray[15],longTermFuelTrimBank2Vector,arrowFileIndexI);
		storeAsStrBytes(strArray[16],fuelTypeVector,arrowFileIndexI);
		storeAsFloat(strArray[17],airIntakeTempVector,arrowFileIndexI);
		storeAsFloat(strArray[18],fuelPressureVector,arrowFileIndexI);
		storeAsFloat(strArray[19],speedVector,arrowFileIndexI);
		storeAsFloat(strArray[20],shortTermFuelTrimBank2Vector,arrowFileIndexI);
		storeAsFloat(strArray[21],shortTermFuelTrimBank1Vector,arrowFileIndexI);
		storeAsStrBytes(strArray[22],engineRuntimeVector,arrowFileIndexI);
		storeAsFloat(strArray[23],throttlePosVector,arrowFileIndexI);
		storeAsStrBytes(strArray[24],dtcNumberVector,arrowFileIndexI);
		storeAsStrBytes(strArray[25],troubleCodesVector,arrowFileIndexI);
		storeAsFloat(strArray[26],timingAdvanceVector,arrowFileIndexI);
		storeAsFloat(strArray[27],equivRatioVector,arrowFileIndexI);
		storeAsInteger(strArray[28],minVector,arrowFileIndexI);
		storeAsInteger(strArray[29],hoursVector,arrowFileIndexI);
		storeAsInteger(strArray[30],daysOfWeekVector,arrowFileIndexI);
		storeAsInteger(strArray[31],monthsVector,arrowFileIndexI);
		storeAsInteger(strArray[32],yearVector,arrowFileIndexI);
		storeAsBit(strArray[33],classVector,arrowFileIndexI);
		return true;
	}

	//
	// Add a "bit" to a BitVector at the given index
	// Very similar to storeAsInteger, but the only acceptable values are 0 or 1 or null
	//
	private void storeAsBit(String strI,BitVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			// Here's another way to do it
			// vecI.setSafe(indexI, 0, 0);
			vecI.setNull(indexI);
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
			vecI.setNull(indexI);
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
			// vecI.setSafe(indexI, 0, -999);
			vecI.setNull(indexI);
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
			vecI.setNull(indexI);
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
			// vecI.setSafe(indexI, 0, -999);
			vecI.setNull(indexI);
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
			vecI.setNull(indexI);
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
			// vecI.setSafe(indexI, 0, -999);
			vecI.setNull(indexI);
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
			vecI.setNull(indexI);
		} else {
			vecI.setSafe(indexI, floatVal);
		}
	}

	//
	// Add string data (as bytes) to a VarCharVector at the given index
	//
	private void storeAsStrBytes(String strI,VarCharVector vecI,int indexI) {
		if ( (strI == null) || (strI.trim().isEmpty()) ) {
			vecI.setNull(indexI);
		} else {
			vecI.setSafe(indexI, strI.trim().getBytes(StandardCharsets.UTF_8));
		}
	}
	
} //end class OBD2Arrow
