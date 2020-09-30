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

package erigo.ct2arrow;

import cycronix.ctlib.CTdata;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class IntDataContainer extends DataContainer {

    public IntVector vec;

    public IntDataContainer(String arrow_chanNameI, String ct_chanNameI, RootAllocator allocatorI) throws Exception {
        super(arrow_chanNameI, ct_chanNameI, CT2Arrow.DataType.INT_DATA);
        vec = new IntVector(arrow_chanName,allocatorI);
        vec.allocateNew(100);
        fieldVec = vec;
        field = vec.getField();
    }

    public void reset() {
        vec.reset();
    }

    public void setValueCount(int recordsInBatchI) {
        vec.setValueCount(recordsInBatchI);
    }

    //
    // Add a single datapoint to our vector.
    // Look through the given CTdata for a datapoint whose time matches the given timestamp.
    // If a matching timestamp is found, add that data to our vector. Otherwise, add
    // null to the vector at this index.
    //
    public void addDataToVector(CTdata ctDataI,int vec_indexI,double timestampI) {
        if (ctDataI == null) {
            vec.setSafe(vec_indexI, 0, -999);
            return;
        }
        double[] times = ctDataI.getTime();
        int[] data = ctDataI.getDataAsInt32();
        int data_index = -1;
        for (int i = 0; i<times.length; ++i) {
            if ( Math.abs(times[i] - timestampI) < 0.0001 ) {
                // We've got a match!
                data_index = i;
                break;
            }
        }
        if (data_index == -1) {
            // Store null at this index in the vector
            System.err.println("Channel " + arrow_chanName + ": didn't find timestamp " + timestampI + " in the given CTdata structure; store null");
            vec.setSafe(vec_indexI, 0, -999);
        } else {
            // TEMPORARY DEBUG
            // if (arrow_chanName.equals("time")) {
            //     System.err.println("  Data on time channel: time[" + vec_indexI + "]: " + timestampI + "," + data[data_index]);
            // }
            vec.setSafe(vec_indexI, data[data_index]);
        }
    }

}
