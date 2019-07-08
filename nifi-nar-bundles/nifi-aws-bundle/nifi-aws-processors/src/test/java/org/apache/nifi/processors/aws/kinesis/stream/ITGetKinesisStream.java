package org.apache.nifi.processors.aws.kinesis.stream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;

import static com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY;

public class ITGetKinesisStream {

    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final String KINESIS_STREAM_NAME = "test-stream";
    private static final String APPLICATION_NAME = "test-application";
    private static final String ENDPOINT_OVERRIDE = "http://localhost:4568";
    private static final String DYNAMODB_ENDPOINT_OVERRIDE = "http://localhost:4569";
    private static final String REGION = Regions.US_EAST_1.toString();

    private AWSCredentialsProvider awsCredentialsProvider =
            new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));

    private AwsClientBuilder.EndpointConfiguration endpointConfiguration =
            new AwsClientBuilder.EndpointConfiguration(ENDPOINT_OVERRIDE, Regions.US_EAST_1.toString());

    private TestRunner runner;

    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws InterruptedException {
        System.setProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

        kinesis = AmazonKinesisClient.builder()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(endpointConfiguration)
                .build();

        kinesis.createStream(KINESIS_STREAM_NAME, 1);

        waitForKinesisToInitialize();

        runner = TestRunners.newTestRunner(GetKinesisStream.class);
        runner.setProperty(GetKinesisStream.APPLICATION_NAME, APPLICATION_NAME);
        runner.setProperty(GetKinesisStream.KINESIS_STREAM_NAME, KINESIS_STREAM_NAME);
        runner.setProperty(GetKinesisStream.ACCESS_KEY, ACCESS_KEY);
        runner.setProperty(GetKinesisStream.SECRET_KEY, SECRET_KEY);
        runner.setProperty(GetKinesisStream.ENDPOINT_OVERRIDE, ENDPOINT_OVERRIDE);
        runner.setProperty(GetKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE, DYNAMODB_ENDPOINT_OVERRIDE);
        runner.setProperty(GetKinesisStream.CLOUDWATCH_ENDPOINT_FLAG, "false");
        runner.assertValid();
    }

    private void waitForKinesisToInitialize() throws InterruptedException {
        Thread.sleep(1000);
    }

    @After
    public void tearDown() {
        kinesis.deleteStream(new DeleteStreamRequest().withStreamName(KINESIS_STREAM_NAME));

        AmazonDynamoDB dynamoDB = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DYNAMODB_ENDPOINT_OVERRIDE, REGION))
                .build();

        dynamoDB.deleteTable(APPLICATION_NAME);

        System.clearProperty(AWS_CBOR_DISABLE_SYSTEM_PROPERTY);

        runner = null;
        kinesis = null;
    }

    @Test
    public void readHorizon() throws InterruptedException, IOException {
        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("horizon".getBytes()), "1");

        initializeKCL(runner, InitialPositionInStream.TRIM_HORIZON);

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertContentEquals("horizon".getBytes());
    }

    @Test
    public void readLatest() throws InterruptedException, IOException {
        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("horizon".getBytes()), "1");

        initializeKCL(runner, InitialPositionInStream.LATEST);

        kinesis.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap("latest".getBytes()), "1");

        waitForKCLToProcessTheLatestMessage();

        runner.assertAllFlowFilesTransferred(PutKinesisStream.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutKinesisStream.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        out.assertContentEquals("latest".getBytes());
    }

    private void waitForKCLToProcessTheLatestMessage() throws InterruptedException {
        Thread.sleep(5_000);
    }

    private void initializeKCL(TestRunner runner, InitialPositionInStream initialPositionInStream) throws InterruptedException {
        runner.setProperty(GetKinesisStream.INITIAL_STREAM_POSITION, initialPositionInStream.name());

        runner.assertValid();

        Executors.newSingleThreadScheduledExecutor().submit((Runnable) runner::run);

        Thread.sleep(20_000);
    }

    // TODO Batch test
    // TODO Timeout test
    // TODO Buffer size test
    // TODO Position test LATEST
    // TODO Position test TRIM_HORIZON
}
