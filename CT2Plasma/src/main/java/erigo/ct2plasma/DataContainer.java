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

package erigo.ct2plasma;

import cycronix.ctlib.CTdata;
import org.apache.arrow.vector.BaseValueVector;

public abstract class DataContainer {

    public String ct_chanName = null; // name of the input CT channel
    public String arrow_chanName = null;    // name of the output channel for the Arrow record batch
    public CT2Plasma.DataType chanType;
    // public BaseValueVector vec = null;
    CTdata ctData = null;

    public DataContainer(String arrow_chanNameI,CT2Plasma.DataType chanTypeI) throws Exception {
        if ( (arrow_chanNameI == null) || (arrow_chanNameI.isEmpty()) ) {
            throw new Exception("DataContainer: Illegal channel name");
        }
        arrow_chanName = arrow_chanNameI;
        if (chanTypeI == CT2Plasma.DataType.INT_DATA) {
            ct_chanName = new String(arrow_chanName + ".i32");
        } else if (chanTypeI == CT2Plasma.DataType.FLOAT_DATA) {
            ct_chanName = new String(arrow_chanName + ".f32");
        } else if (chanTypeI == CT2Plasma.DataType.STRING_DATA) {
            ct_chanName = new String(arrow_chanName + ".csv");
        }
        chanType = chanTypeI;
    }

    public abstract void reset();

    public abstract void setValueCount(int recordsInBatchI);

    public abstract void addDataToVector(CTdata ctDataI,int vec_indexI,double timestampI);

}
