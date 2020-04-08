package com.openlattice.testing.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.io.Resources
import io.findify.s3mock.S3Mock
import java.net.URL
import java.nio.file.Paths

class S3TestingUtils {

    companion object {
        @JvmStatic
        fun newTestS3Client(port: Int, region: String): AmazonS3 {
            val api = S3Mock.Builder().withPort(port).withInMemoryBackend().build()
            api.start()
            val endpoint = AwsClientBuilder.EndpointConfiguration("http://localhost:$port", region)
            val client = AmazonS3ClientBuilder
                    .standard()
                    .withPathStyleAccessEnabled(true)
                    .withEndpointConfiguration(endpoint)
                    .withCredentials(AWSStaticCredentialsProvider(AnonymousAWSCredentials()))
                    .build();
            return client
        }

        @JvmStatic
        fun loadS3WithResourceAsFile(bucket: String, key: String, resourcePath: String, client: AmazonS3) {
            val testResource: URL = Resources.getResource(resourcePath)
            val testResAsFile = Paths.get(testResource.toURI()).toFile()
            client.putObject(bucket, key, testResAsFile)
        }
    }

}
