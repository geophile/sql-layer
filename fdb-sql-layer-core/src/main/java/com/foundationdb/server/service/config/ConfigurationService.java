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
package com.foundationdb.server.service.config;

import java.util.Map;
import java.util.Properties;

public interface ConfigurationService
{
    /**
     * Gets the specified property.
     * @param propertyName the property name
     * @return the specified property's value
     * @throws PropertyNotDefinedException if the given module and property are not defined.
     */
    String getProperty(String propertyName) throws PropertyNotDefinedException;

    /**
     * <p>Creates a {@code java.util.Properties} file that reflects all known properties whose keys start with the given
     * prefix. That prefix will be omitted from the keys of the resulting Properties.</p>
     * <p>For instance, if this ConfigurationService had defined properties:
     * <table border="1">
     *     <tr>
     *         <th>key</th>
     *         <th>value</th>
     *     </tr>
     *     <tr>
     *         <td>a.one</td>
     *         <td>1</td>
     *     </tr>
     *     <tr>
     *         <td>a.one.alpha</td>
     *         <td>1a</td>
     *     </tr>
     *     <tr>
     *         <td>a.two</td>
     *         <td>2</td>
     *     </tr>
     *     <tr>
     *         <td>b.three</td>
     *         <td>3</td>
     *     </tr>
     * </table>
     * ...then {@code deriveProperties(a.)} would result in a Properties instance with key-value pairs:
     * <table border="1">
     *     <tr>
     *         <th>key</th>
     *         <th>value</th>
     *     </tr>
     *     <tr>
     *         <td>one</td>
     *         <td>1</td>
     *     </tr>
     *     <tr>
     *         <td>one.alpha</td>
     *         <td>1a</td>
     *     </tr>
     *     <tr>
     *         <td>two</td>
     *         <td>2</td>
     *     </tr>
     * </table>
     * </p>
     * @param withPrefix the key prefix which acts as both a selector and eliding force of keys
     * @return the derived Properties instance, which may be safely altered
     * @throws NullPointerException if withPrefix is null
     */
    Properties deriveProperties(String withPrefix);

    /**
     * Get all of the defined properties as an immutable Map.
     * @return a Map of all defined properties
     */
    Map<String,String> getProperties();

    long queryTimeoutMilli();
    void queryTimeoutMilli(long queryTimeoutMilli);
    String getInstanceID();
    boolean testing();
}
