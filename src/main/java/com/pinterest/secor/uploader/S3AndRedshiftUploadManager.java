package com.pinterest.secor.uploader;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.FileUtil;

public class S3AndRedshiftUploadManager extends S3UploadManager {
    public S3AndRedshiftUploadManager(SecorConfig config) {
        super(config);
    }

    @Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        // Handle both S3 and Redshift uploads
        Handle s3UploadHandle = super.upload(localPath);

        String s3Path = getS3Path(localPath);
        String table = localPath.getTopic();
        String database = System.getProperty("redshift.db");
        String host = System.getProperty("redshift.host");
        Integer port = Integer.parseInt(System.getProperty("redshift.port"));
        String username = System.getProperty("redshift.username");
        String password = System.getProperty("redshift.password");
        String formatOptions = System.getProperty("redshift.formatoptions");

        RedshiftCopier redshiftCopier = new RedshiftCopier(s3Path, host, port, username, password,
                                        database, table, formatOptions);

        return new S3AndRedshiftUploadHandle(s3UploadHandle, redshiftCopier);
    }

    private String getS3Path(LogFilePath localPath) {
        String s3Key;

        if (mConfig.getS3MD5HashPrefix()) {
            // add MD5 hash to the prefix to have proper partitioning of the secor logs on s3
            String md5Hash = FileUtil.getMd5Hash(localPath.getTopic(), localPath.getPartitions());
            s3Key = localPath.withPrefix(md5Hash + "/" + mConfig.getS3Path()).getLogFilePath();
        }
        else {
            s3Key = localPath.withPrefix(mConfig.getS3Path()).getLogFilePath();
        }

        return "s3://" + mConfig.getS3Bucket() + "/" + s3Key;
    }
}
