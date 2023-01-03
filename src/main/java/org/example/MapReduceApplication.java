package org.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;

import java.io.File;

public class MapReduceApplication {
    public static void main(String[] args) throws Exception {
        // Replace these with your own AWS access key and secret key

        AWSCredentials credentials = new PropertiesCredentials(new File("src/main/resources/cred.txt"));

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        // Upload the JAR file to S3
        File jarFile = new File("out/artifacts/DeleteEstimation_jar/DeleteEstimation.jar");
        String jarKey = "DeleteEstimation.jar ";
        String bucketName = "jarbucketformapreduce";
        PutObjectRequest putRequest = new PutObjectRequest(bucketName, jarKey, jarFile);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/java-archive");
        putRequest.setMetadata(metadata);
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).build();
        Upload upload = tm.upload(putRequest);
        upload.waitForCompletion();

        // Create an Amazon EMR client
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        // Set up the job flow
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarKey) // specify the JAR file uploaded to S3
                .withMainClass("com.example.Main") // specify the main class of your JAR file
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/3gram/data", "s3://jarbucketformapreduce/Output1", "s3://jarbucketformapreduce/eng-stopwords.txt"); // specify any command-line arguments to be passed to the main class
        Application hadoopApplication = new Application()
                .withName("Hadoop"); // specify a name for the application

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("My Job Flow") // specify a name for the job flow
                .withReleaseLabel("emr-4.2.0") // specify the EMR release version to use
                .withApplications(hadoopApplication) // add the Hadoop application to the job flow
                .withSteps(new StepConfig().withName("Word count").
                        withHadoopJarStep(hadoopJarStep).withActionOnFailure(ActionOnFailure.CONTINUE)) // add the JAR step to the job flow
                .withLogUri("s3://aws-logs-216937418381-us-east-1") // specify a bucket for EMR to store log files
                .withInstances(new JobFlowInstancesConfig()
                        .withInstanceCount(2) // specify the number of instances to use
                        .withKeepJobFlowAliveWhenNoSteps(false) // shut down the cluster when the job flow completes
                        .withMasterInstanceType("m4.large") // specify the type of instance to use for the master node
                        .withSlaveInstanceType("m4.large")); // specify the type of instance to use for the slave nodes

// Run the job flow
        RunJobFlowResult result = emr.runJobFlow(request);
        String jobFlowId = result.getJobFlowId();
        System.out.println("Started job flow with ID: " + jobFlowId);
    }
}