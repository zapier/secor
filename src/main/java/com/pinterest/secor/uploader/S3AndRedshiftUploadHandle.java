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
package com.pinterest.secor.uploader;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class S3AndRedshiftUploadHandle implements Handle<Boolean> {
    private Handle mS3UploadHandle;
    private Future<Void> mRedshiftCopyHandle;
    private ExecutorService executor;

    private class RedshiftCopyExecutor implements Callable<Void> {
        private RedshiftCopier mRedshiftCopier;

        RedshiftCopyExecutor(RedshiftCopier redshiftCopier) {
            this.mRedshiftCopier = redshiftCopier;
        }

        @Override
        public Void call() throws Exception {
            int copyStatus = mRedshiftCopier.copy();
            if(copyStatus != 0) {
                throw new RuntimeException("Redshift copy failed with status: " + copyStatus);
            }

            return null;
        }
    }

    public S3AndRedshiftUploadHandle(Handle s3UploadHandle, RedshiftCopier redshiftCopier) {
        mS3UploadHandle = s3UploadHandle;
        executor = Executors.newSingleThreadExecutor();
        mRedshiftCopyHandle = executor.submit(new RedshiftCopyExecutor(redshiftCopier));
    }

    public Boolean get() throws Exception {
        mS3UploadHandle.get();
        mRedshiftCopyHandle.get();
        executor.shutdown();
        return true;
    }
}
