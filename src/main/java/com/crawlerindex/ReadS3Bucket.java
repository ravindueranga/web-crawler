package com.crawlerindex;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class ReadS3Bucket {

	public void process() {

		ArrayList<String> nameList = readFileToList();

		AmazonS3 s3 = new AmazonS3Client();
		ObjectListing listing = new ObjectListing();

		ExtractWarc extractWarc = new ExtractWarc();

		for (int i = 0; i < nameList.size(); i++) {
			System.out.println("Reading from " + nameList.get(i));

			listing = s3.listObjects("commoncrawl", nameList.get(i));

			List<S3ObjectSummary> summaries = listing.getObjectSummaries();

			for (S3ObjectSummary summary : summaries) {
				try {
					String key = summary.getKey();
					S3Object object = s3.getObject(new GetObjectRequest("commoncrawl", key));
					InputStream objectData = object.getObjectContent();
					GZIPInputStream gzInputStream = new GZIPInputStream(objectData);
					DataInputStream inStream = new DataInputStream(gzInputStream);
					extractWarc.process(inStream);
					objectData.close();

				} catch (Exception e) {

				}
			}
		}
	}

	private ArrayList<String> readFileToList() {

		ArrayList<String> nameList = new ArrayList<>();
		try {
			InputStream fstream = getClass().getResourceAsStream("/warcnamelist.txt");

			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;

			//Read File Line By Line
			while ((strLine = br.readLine()) != null) {
				nameList.add(strLine);
			}
			//Close the input stream
			fstream.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		return nameList;
	}

}
