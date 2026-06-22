package com.firebolt.jdbc.client.query.response;

import com.firebolt.jdbc.client.query.response.s3.AwsInfo;
import com.firebolt.jdbc.client.query.response.s3.S3DownloaderType;
import com.firebolt.jdbc.client.query.response.s3.S3ResponseReader;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementType;

public final class ResponseReaderProvider {
    private ResponseReaderProvider() {
    }

    private static class InstanceHolder {
        private static final ResponseReaderProvider INSTANCE = new ResponseReaderProvider();
    }

    public static ResponseReaderProvider getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public ResponseReader getResponseReader(FireboltProperties fireboltProperties, StatementType statementType) {

        // only use the s3 reader in case the statement is a query
        if (fireboltProperties.getQueryResultLocation() != null && statementType == StatementType.QUERY) {
            AwsInfo awsInfo = AwsInfo.builder()
                    .region(fireboltProperties.getAwsRegion())
                    .keyId(fireboltProperties.getAwsAccessKeyId())
                    .keySecret(fireboltProperties.getAwsSecretAccessKey())
                    .sessionToken(fireboltProperties.getAwsSessionToken())
                    .build();
            boolean isResponseCompressed = fireboltProperties.isCompress();
            return new S3ResponseReader(awsInfo, isResponseCompressed, S3DownloaderType.fromValue(fireboltProperties.getFileDownloaderType()));
        }

        return new DefaultResponseReader();
    }
}
