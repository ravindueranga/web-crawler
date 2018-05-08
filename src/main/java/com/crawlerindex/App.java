package com.crawlerindex;


public class App {

    public static void main(final String[] arg) throws Exception
    {
        try {
            ReadS3Bucket readS3Bucket = new ReadS3Bucket();

            readS3Bucket.process();
        }catch(Exception e){
            System.out.println("Exception Occured");
        }


    }



}