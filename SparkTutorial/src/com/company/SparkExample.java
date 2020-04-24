package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * @author soham chakraborti
 * Use spark to do various tasks in like word count etc
 * Sample example
 */
public class SparkExample {
    /**
     * Main driver method.
     * @param args inputfile is an argument.
     */
    public static void main(String[] args) {
        // seperation of concerns
        processPart2(args[0]);
    }

    /**
     *  Process the inputfile and solve the tasks.
     * @param inputFileName
     */
    private static void processPart2(String inputFileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local")
                .setAppName("JD part2 HW5")
                .set("spark.local.dir", "/tmp/spark-temp");;

        // create the spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // process the input file
        JavaRDD<String> inputFile = sparkContext.textFile(inputFileName);

        // using count to get number of lines in the input file.
        System.out.println("line count :" + inputFile.count());

        JavaRDD<String> wordsInFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));

        // using count to get number of words in the input file.
        System.out.println("word count :" + wordsInFile.count());

        // using count and distinct to get number of unique words in the input file.
        System.out.println("Distinct word count :" + wordsInFile.distinct().count());

      
        JavaPairRDD wordDigit1 = wordsInFile.mapToPair(word -> new Tuple2(word, 1));

        // save each word with a count 1
        wordDigit1.saveAsTextFile("../SampleOutDir1");

       //reduceby key, takes same keys and applies the operation
        JavaPairRDD wordCount = wordDigit1.reduceByKey((counter1, counter2) -> (int)counter1 + (int)counter2);
        wordCount.saveAsTextFile("../SampleOutDir2");

        // search user input from RDD
        System.out.println("Enter a word to search >");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            String wordToSearch = bufferedReader.readLine();
            inputFile.foreach(line -> {
                if (line.contains(wordToSearch)) {
                    // matched line.
                    System.out.println(line);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
