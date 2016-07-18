/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JSONFileReaderWriterFactoryTest {
    private JSONFileReaderWriterFactory mFactory;

    public void setUp() throws Exception {
        mFactory = new JSONFileReaderWriterFactory();
    }

    @Test
    public void testJSONReadWriteRoundTrip() throws Exception {
        JSONFileReaderWriterFactory factory = new JSONFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-json-topic",
                new String[]{"part-1"},
                0,
                1,
                0,
                ".log"
        );

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, null);
        String jsonStr1 = "{\"id\":1,\"timestamp\":\"2016-02-15 14:05:06.123\",\"ip\":\"192.168.1.110\"," +
                          "\"deleted\":false}";
        String jsonStr2 = "{\"id\":1,\"timestamp\":\"2016-02-15 15:05:06.123\",\"ip\":\"192.168.1.210\"," +
                          "\"deleted\":true}";

        KeyValue kv1 = (new KeyValue(23232, jsonStr1.getBytes()));
        KeyValue kv2 = (new KeyValue(23233, jsonStr2.getBytes()));
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());

        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
    }
}
