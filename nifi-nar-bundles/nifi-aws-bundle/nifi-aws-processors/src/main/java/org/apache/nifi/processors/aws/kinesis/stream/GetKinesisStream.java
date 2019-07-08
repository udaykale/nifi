package org.apache.nifi.processors.aws.kinesis.stream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

@Tags({"amazon", "aws", "kinesis", "get", "stream"})
@CapabilityDescription("Reads data from the specified AWS Kinesis stream name. " +
        "Uses DynamoDB for check pointing and CloudWatch for metrics. " +
        "Ensure that the credentials provided have access to DynamoDB and CloudWatch along with Kinesis.")
@SeeAlso(PutKinesisStream.class)
public class GetKinesisStream extends AbstractKinesisStreamProcessor {

    // TODO ask for encoding?

    // TODO: proper documentation
    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Application Name")
            .name("amazon-kinesis-stream-application-name")
            .description("The application name.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true).build();

    // TODO: proper documentation
    public static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Initial Stream Position")
            .name("amazon-kinesis-stream-initial-position")
            .description("Initial position to read getLogger()s from")
            .allowableValues(InitialPositionInStream.LATEST.toString(),
                    InitialPositionInStream.TRIM_HORIZON.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(InitialPositionInStream.LATEST.toString())
            .required(false).build();

    // TODO: proper documentation
    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis DynamoDB Override")
            .name("amazon-kinesis-stream-dynamodb-override")
            .description("DynamoDB override to use non AWS deployments")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false).build();

    // TODO: proper documentation
    public static final PropertyDescriptor CLOUDWATCH_ENDPOINT_FLAG = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Cloudwatch Flag")
            .name("amazon-kinesis-stream-cloudwatch-flag")
            .description("DynamoDB override to use non AWS deployments")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false).build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KINESIS_STREAM_NAME, BATCH_SIZE, MAX_MESSAGE_BUFFER_SIZE_MB, REGION,
                    ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE,
                    TIMEOUT, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME,
                    PROXY_PASSWORD, ENDPOINT_OVERRIDE, APPLICATION_NAME, INITIAL_STREAM_POSITION,
                    DYNAMODB_ENDPOINT_OVERRIDE, CLOUDWATCH_ENDPOINT_FLAG));

    private Worker worker;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        String workerId;
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new ProcessException(e);
        }

        String applicationName =
                StringUtils.trimToEmpty(context.getProperty(APPLICATION_NAME).getValue());
        String streamName =
                StringUtils.trimToEmpty(context.getProperty(KINESIS_STREAM_NAME)
                        .evaluateAttributeExpressions().getValue());
        String dynamoDBEndpointOverride =
                StringUtils.trimToEmpty(context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE)
                        .evaluateAttributeExpressions().getValue());
        String kinesisEndpointOverride =
                StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE)
                        .evaluateAttributeExpressions().getValue());
        String initialPositionStr =
                StringUtils.trimToEmpty(context.getProperty(INITIAL_STREAM_POSITION).getValue());

        InitialPositionInStream initialPosition = InitialPositionInStream.valueOf(initialPositionStr);

        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(getCredentials(context));

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(applicationName, streamName, awsCredentialsProvider, workerId)
                        .withInitialPositionInStream(initialPosition);

        AmazonDynamoDB dynamoClient = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(new EndpointConfiguration(dynamoDBEndpointOverride, region.getName()))
                .build();

        AmazonKinesis kinesisClient = AmazonKinesisClient.builder()
                .withEndpointConfiguration(new EndpointConfiguration(kinesisEndpointOverride, region.getName()))
                .build();

        worker = new Worker.Builder().recordProcessorFactory(new RecordProcessorFactory(session))
                .config(kinesisClientLibConfiguration)
                .dynamoDBClient(dynamoClient)
                .kinesisClient(kinesisClient)
                .metricsFactory(new NullMetricsFactory())
                .build();

        System.out.printf("Running %s to process stream %s as worker %s...\n", applicationName, streamName, workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }

        System.exit(exitCode);
    }

    @Override
    public void onShutDown() {
        // TODO write proper shutdown logic
        worker.startGracefulShutdown();
    }

    /**
     * Used to create new record processors.
     */
    public class RecordProcessorFactory implements IRecordProcessorFactory {
        private ProcessSession session;

        public RecordProcessorFactory(ProcessSession session) {
            this.session = session;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public IRecordProcessor createProcessor() {
            return new RecordProcessor(session);
        }
    }

    /**
     * Processes records and checkpoints progress.
     */
    public static class RecordProcessor implements IRecordProcessor {

        private static final Log LOG = LogFactory.getLog(RecordProcessor.class);
        private String kinesisShardId;

        // Backoff and retry settings
        private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
        private static final int NUM_RETRIES = 10;

        // Checkpoint about once a minute
        private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
        private long nextCheckpointTimeInMillis;

        private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

        private ProcessSession session;

        public RecordProcessor(ProcessSession session) {
            this.session = session;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void initialize(String shardId) {
            LOG.info("Initializing record processor for shard: " + shardId);
            this.kinesisShardId = shardId;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
            LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

            // Process records and perform all exception handling.
            processRecordsWithRetries(records);

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(checkpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        }

        /**
         * Process records performing retries as needed. Skip "poison pill" records.
         *
         * @param records Data records to be processed.
         */
        private void processRecordsWithRetries(List<Record> records) {
            List<FlowFile> successfulFlowFiles = new LinkedList<>();

            for (Record record : records) {
                boolean processedSuccessfully = false;
                for (int i = 0; i < NUM_RETRIES; i++) {
                    try {
                        //
                        // Logic to process record goes here.
                        //
                        FlowFile flowFile = processSingleRecord(record);
                        successfulFlowFiles.add(flowFile);

                        processedSuccessfully = true;
                        break;
                    } catch (Throwable t) {
                        LOG.warn("Caught throwable while processing record " + record, t);
                    }

                    // backoff if we encounter an exception.
                    try {
                        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.debug("Interrupted sleep", e);
                    }
                }

                if (!processedSuccessfully) {
                    LOG.error("Couldn't process record " + record + ". Skipping the record.");
                }
            }

            session.transfer(successfulFlowFiles, REL_SUCCESS);
        }

        /**
         * Process a single record.
         *
         * @param record The record to be processed.
         * @return
         */
        private FlowFile processSingleRecord(Record record) {
            // TODO Send data into a flow file

            String data = null;
            FlowFile flowFile = session.create();

            try {
                // For this app, we interpret the payload as UTF-8 chars.
                data = decoder.decode(record.getData()).toString();

                session.append(flowFile, out -> out.write(record.getData().array()));

                // Assume this record came from AmazonKinesisSample and log its age.

                LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data);
            } catch (NumberFormatException e) {
                LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
            } catch (CharacterCodingException e) {
                LOG.error("Malformed data: " + data, e);
            }

            return flowFile;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
            LOG.info("Shutting down record processor for shard: " + kinesisShardId);
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            if (reason == ShutdownReason.TERMINATE) {
                checkpoint(checkpointer);
            }
        }

        /**
         * Checkpoint with retries.
         *
         * @param checkpointer
         */
        private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
            LOG.info("Checkpointing shard " + kinesisShardId);
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    checkpointer.checkpoint();
                    break;
                } catch (ShutdownException se) {
                    // Ignore checkpoint if the processor instance has been shutdown (fail over).
                    LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                    break;
                } catch (ThrottlingException e) {
                    // Backoff and re-attempt checkpoint upon transient failures
                    if (i >= (NUM_RETRIES - 1)) {
                        LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                        break;
                    } else {
                        LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                                + NUM_RETRIES, e);
                    }
                } catch (InvalidStateException e) {
                    // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                    LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                    break;
                }
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }
        }
    }
}
