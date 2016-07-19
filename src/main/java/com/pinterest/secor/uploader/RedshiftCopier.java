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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class RedshiftCopier {
    private static final Logger LOG = LoggerFactory.getLogger(RedshiftCopier.class);

    private final String mS3Path;
    private final String mHost;
    private final int mPort;
    private final String mUsername;
    private final String mPassword;
    private final String mDatabase;
    private final String mTable;
    private final String mFormatOptions;

    public RedshiftCopier(String s3Path, String host, int port, String username, String password,
                          String database, String table, String formatOptions) {
        this.mS3Path = s3Path;
        this.mHost = host;
        this.mPort = port;
        this.mUsername = username;
        this.mPassword = password;
        this.mDatabase = database;
        this.mTable = table;
        this.mFormatOptions = formatOptions;
    }

    public int copy() throws Exception {
        Class.forName("org.postgresql.Driver");

        String dbURL = "jdbc:postgresql://"+mHost+":"+mPort+"/"+mDatabase;
        Properties props = new Properties();
        props.setProperty("user", mUsername);
        props.setProperty("password", mPassword);
        props.setProperty("connectTimeout", "30");
        props.setProperty("socketTimeout", "3600");

        LOG.info("Connecting to Redshift...");
        Connection conn = DriverManager.getConnection(dbURL, props);

        String aws_access_key_id = System.getenv("aws_access_key_id");
        String aws_secret_access_key = System.getenv("aws_secret_access_key");

        Statement stmt = conn.createStatement();
        String sql = "COPY " + mTable + " FROM '"+mS3Path+"' WITH CREDENTIALS AS " +
                    "'aws_access_key_id="+aws_access_key_id+";aws_secret_access_key="+aws_secret_access_key+"' " +
                    mFormatOptions + ";";

        LOG.info("Copying file into Redshift...");
        int status = stmt.executeUpdate(sql);
        LOG.info("Copy return status: " + status);

        stmt.close();
        conn.close();

        return status;
    }
}
