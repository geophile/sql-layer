/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;

public interface NewAISProvider {
    /**
     * Gets the AIS that's been built.
     * @return the AIS
     */

    AkibanInformationSchema ais();
    /**
     * Gets the AIS that's been built.
     * @param freezeAIS whether to freeze the AIS before returning it
     * @return the AIS
     */
    AkibanInformationSchema ais(boolean freezeAIS);

    /**
     * Gets the AIS that's been built, but without performing
     * AIS validations. Used for building test schemas which may 
     * be invalid, but that's ok for testing purposes.  
     * @return the AIS
     */
    AkibanInformationSchema unvalidatedAIS();

    /**
     * <p>Defines (but does not yet start building) a group index.</p>
     *
     * <p>Note that this puts you into the realm of a cousin interface branch;
     * you can't alter the main schema anymore. This is by design, as implementations may need to differentiate
     * between structural building and building that depends on a stable structure (such as group index creation).</p>
     * @param indexName the new index's name
     * @param joinType the new index's join type
     * @return the group index builder
     */
    NewAISGroupIndexStarter groupIndex(String indexName, Index.JoinType joinType);
}
