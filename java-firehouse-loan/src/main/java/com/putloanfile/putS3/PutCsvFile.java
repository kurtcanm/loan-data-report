// Referenced from https://github.com/aws-samples/aws-java-sample
package com.putloanfile.putS3;

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class PutCsvFile
 {
  public static void main(String[] args) throws IOException {

          AmazonS3 s3 = new AmazonS3Client();
          Region euCentral1 = Region.getRegion(Regions.EU_CENTRAL_1);
          s3.setRegion(euCentral1);

          String bucketName = "s3-loan-stream";
          String localFilePath = "loan.csv";
          String key = "raw_data.csv";

          try {
            s3.putObject(new PutObjectRequest(bucketName, key, new File(localFilePath)));

          } catch (AmazonServiceException ase) {
              System.out.println("Caught an AmazonServiceException, which means your request made it "
                      + "to Amazon S3, but was rejected with an error response for some reason.");
              System.out.println("Error Message:    " + ase.getMessage());
              System.out.println("HTTP Status Code: " + ase.getStatusCode());
              System.out.println("AWS Error Code:   " + ase.getErrorCode());
              System.out.println("Error Type:       " + ase.getErrorType());
              System.out.println("Request ID:       " + ase.getRequestId());
          } catch (AmazonClientException ace) {
              System.out.println("Caught an AmazonClientException, which means the client encountered "
                      + "a serious internal problem while trying to communicate with S3, "
                      + "such as not being able to access the network.");
              System.out.println("Error Message: " + ace.getMessage());
          }
      }
  }

