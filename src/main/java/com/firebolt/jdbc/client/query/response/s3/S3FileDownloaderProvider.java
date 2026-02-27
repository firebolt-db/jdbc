package com.firebolt.jdbc.client.query.response.s3;

public class S3FileDownloaderProvider {

    public S3FileDownloader get(S3DownloaderType type) {
        switch(type) {
            case SEQUENTIAL:
                return new SequentialS3FileDownloader();
            case PARALLEL:
                return new ParallelS3FileDownloader();
            case ONE_AHEAD:
            default:
                return new OneAheadS3FileDownloader();
        }
    }
}
