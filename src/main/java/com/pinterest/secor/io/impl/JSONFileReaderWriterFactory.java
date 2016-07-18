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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.EOFException;


/**
 * JSON file reader writer implementation
 *
 * @author Kishore Nallan (kishore.nallan@zapier.com)
 */
public class JSONFileReaderWriterFactory implements FileReaderWriterFactory {
    private static final byte DELIMITER = '\n';
    private static final String OFFSET_KEY = "offset";
    private static final String VALUE_KEY = "value";

    public final static ObjectMapper mapper = new ObjectMapper();

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new JSONFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new JSONFileWriter(logFilePath, codec);
    }

    protected class JSONFileReader implements FileReader {
        private final BufferedInputStream mReader;

        public JSONFileReader(LogFilePath path, CompressionCodec codec) throws Exception {
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            InputStream inputStream = fs.open(fsPath);
            this.mReader = (codec == null) ? new BufferedInputStream(inputStream)
                    : new BufferedInputStream(
                    codec.createInputStream(inputStream, CodecPool.getDecompressor(codec)));
        }

        @Override
        public KeyValue next() throws IOException {
            ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();
            int nextByte;
            while ((nextByte = mReader.read()) != DELIMITER) {
                if (nextByte == -1) { // end of stream?
                    if (messageBuffer.size() == 0) { // if no byte read
                        return null;
                    } else { // if bytes followed by end of stream: framing error
                        throw new EOFException(
                                "Non-empty message without delimiter");
                    }
                }
                messageBuffer.write(nextByte);
            }

            ObjectNode containerNode = (ObjectNode) new ObjectMapper().readTree(messageBuffer.toByteArray());
            JsonNode offsetNode = containerNode.remove(OFFSET_KEY);
            JsonNode valueNode = containerNode.remove(VALUE_KEY);

            return new KeyValue(offsetNode.asLong(), mapper.writeValueAsBytes(valueNode));
        }

        @Override
        public void close() throws IOException {
            this.mReader.close();
        }
    }

    protected class JSONFileWriter implements FileWriter {
        private final CountingOutputStream mCountingStream;
        private final BufferedOutputStream mWriter;

        public JSONFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            this.mCountingStream = new CountingOutputStream(fs.create(fsPath));
            this.mWriter = (codec == null) ? new BufferedOutputStream(
                    this.mCountingStream) : new BufferedOutputStream(
                    codec.createOutputStream(this.mCountingStream, CodecPool.getCompressor(codec)));
        }

        @Override
        public long getLength() throws IOException {
            assert this.mCountingStream != null;
            return this.mCountingStream.getCount();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            // Push the incoming JSON string value into a container that also stores the corresponding offset
            JsonNode offsetNode = mapper.valueToTree(keyValue.getOffset());
            JsonNode valueNode = mapper.readTree(keyValue.getValue());

            ObjectNode wrapperNode = mapper.createObjectNode();
            wrapperNode.set(OFFSET_KEY, offsetNode);
            wrapperNode.set(VALUE_KEY, valueNode);

            String jsonStr = mapper.writeValueAsString(wrapperNode);

            this.mWriter.write(jsonStr.getBytes());
            this.mWriter.write(DELIMITER);
        }

        @Override
        public void close() throws IOException {
            this.mWriter.close();
        }
    }
}
