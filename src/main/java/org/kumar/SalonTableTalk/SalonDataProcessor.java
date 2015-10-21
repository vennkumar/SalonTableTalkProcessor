package org.kumar.SalonTableTalk;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;









//import java.util.Set;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Hello world!
 *
 */
public class SalonDataProcessor implements Serializable
{
    //private static final long serialVersionUID = -1322322139926390329L;
    private static final long serialVersionUID = -13223239926390329L;

	public void Display(SortedSet<String> sortedShingleSet){
		System.out.println("-------------------------------\n");

		for (String shingle : sortedShingleSet){
			System.out.print("|" + shingle);
		}
		System.out.println("\n\n");
	}
	
	public float JaccardSimilarity(SortedSet<String> set1, SortedSet<String> set2){
		SortedSet<String> setUnion = new TreeSet<String>(); 
		setUnion.addAll(set1);
		setUnion.addAll(set2);
		
		SortedSet<String> setIntersection = new TreeSet<String>();
		setIntersection.addAll(set1);
		setIntersection.retainAll(set2);
		
		System.out.println("Size of intersection: " + setIntersection.size());
		System.out.println("Size of Union: " + setUnion.size());

		return (float)setIntersection.size() / (float)setUnion.size();
	}
	
	public SortedSet<String> GenerateShingleSet(String document, int shingleLength){
	//public String[] GenerateShingleSet(String document, int shingleLength){
		int docLen = document.length();
		int numOfShingles = docLen - shingleLength + 1;
		SortedSet<String> sortedShingleSet = new TreeSet<String>();
		for (int i = 0; i < numOfShingles; i++){
			sortedShingleSet.add(document.substring(i, i+shingleLength));
		}
		
		return sortedShingleSet;
	}
	
	public String[] GenerateShingleSetX(String document, int shingleLength){
		SortedSet<String> sortedShingleSet = GenerateShingleSet(document, shingleLength);
		int sizeOfSortedSingleSet = sortedShingleSet.size();
		String[] shinglesArray = new String[sizeOfSortedSingleSet];
		int i = 0;
		for (String shingle : sortedShingleSet){
			shinglesArray[i] = shingle;
			i++;
		}
		return shinglesArray;
	}

	
	
    public static void main( String[] args )
    {
        //String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\tableTalkComments0000Sample.txt"; 
    	//String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\tableTalkComments0.txt";
    	String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\TEST\\tableTalkComments*.txt";
        final int shingleLength=6;
        final int minHashSigLen=25;
        final SalonDataProcessor salonDataProcessor = new SalonDataProcessor();
        
        SparkConf conf = new SparkConf().setAppName("Salon Table Talk Data Processor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> commentsRawData = sc.textFile(dataFile).cache();
        JavaRDD<String> commentsRawData = sc.textFile(dataFile);

        	long commentsCount = commentsRawData.count();
/*        long numAs = commentsRawData.filter(new Function<String, Boolean>() {
          public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = commentsRawData.filter(new Function<String, Boolean>() {
          public Boolean call(String s) { return s.contains("b"); }
        }).count();*/

        System.out.println("\n\n\n\n\n\n\n\n\n\nNumber of lines: " + commentsCount);
        System.out.println("\n\n\n\n\nNumber of lines: " + commentsCount);
        System.out.println("\n\n\n\n\nNumber of lines: " + commentsCount);
        System.out.println("\n\n\n\n\nNumber of lines: " + commentsCount);
        System.out.println("\n\n\n\n\n ");
          
        JavaPairRDD<Integer, SortedSet<String>> docIdCommentPairRDD = commentsRawData.mapToPair(
        		new PairFunction<
        				String, // T as input
        				Integer, // K as output
        				SortedSet<String> // V as output
        		>() {
        			@Override
        			public Tuple2<Integer, SortedSet<String>> call(String element) {
        				String[] tokens = element.split("\\|\\|");
        				SortedSet<String> sortedShingleSet = null;
        				if (tokens.length == 1){
        					sortedShingleSet=new TreeSet<String>();
        					sortedShingleSet.add("XXX");
        				}
        				else
        				try{
        					sortedShingleSet = salonDataProcessor.GenerateShingleSet(tokens[1], shingleLength);
        				} catch(Exception e){
        					System.out.println("Exception while processing: \n" + element );
        					System.out.println(tokens.length);
        					e.printStackTrace();
        				}
        				return new Tuple2<Integer, SortedSet<String>>(new Integer(tokens[0]), sortedShingleSet);
        			}
        });
        
        Tuple2<Integer, SortedSet<String>> tuple2Obj = docIdCommentPairRDD.first();
        System.out.println("\n\n\n\n\n ");
        
        /* 
       Tuple2<Integer, SortedSet<String>> 
        	consolidatedShingles = docIdCommentPairRDD.reduce(
        									new Function2<Tuple2<Integer, SortedSet<String>>, 
        												  Tuple2<Integer, SortedSet<String>>, 
        												  Tuple2<Integer, SortedSet<String>>>(){
        									@Override
        									public Tuple2<Integer, SortedSet<String>> call (Tuple2<Integer, SortedSet<String>> item1, Tuple2<Integer, SortedSet<String>> item2){
        										//Tuple2<Integer, SortedSet<String>> result = new Tuple2<Integer, SortedSet<String>>(shingleLength, null);
        										SortedSet<String> consSet = new TreeSet<String>();
        										consSet.addAll(item1._2);
        										consSet.addAll(item2._2);
        										return new Tuple2<Integer, SortedSet<String>> (item1._1, consSet);
        									}
        									}
        								);
        */
        
        JavaRDD<String> allShinglesRDD = docIdCommentPairRDD.flatMap(new FlatMapFunction<Tuple2<Integer, SortedSet<String>>, String>() {
			@Override
			public Iterable<String> call(Tuple2<Integer, SortedSet<String>> arg0)
					throws Exception {
				List<String> result = new ArrayList<String>();
				for (String elt : arg0._2){
					result.add(elt);
				}
				return result;
			}
          });
        
        JavaRDD<String> distinctAllShingles = allShinglesRDD.distinct();
        

        
        System.out.println("--------------------------------------- ");
        System.out.println(commentsRawData.first());
        System.out.println("--------------------------------------- ");
        System.out.println(tuple2Obj._2.toString());
        System.out.println("--------------------------------------- ");
        System.out.println(tuple2Obj._2.size());
        System.out.println("All Shingles rdd allShinglesRDD size: " + allShinglesRDD.count());
        System.out.println("All Shingles .first.toString: " + allShinglesRDD.first().toString());
        System.out.println("Distinct Shingles count: " + distinctAllShingles.count());
        System.out.println("\n\n\n\n\n ");
        //distinctAllShingles.saveAsTextFile("C:\\bigData\\HadoopFreeSpark\\allShingles");

        JavaPairRDD<String, Integer> allShingleDocIdDistinctRDD = docIdCommentPairRDD
        		.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, SortedSet<String>>, String, Integer>() {
			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, SortedSet<String>> arg0)
					throws Exception {
				List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
				for (String elt : arg0._2){
					result.add(new Tuple2<String, Integer>(elt, arg0._1));
				}
				return result;
			}
          });
        
        JavaPairRDD<String, Iterable<Integer>> allShingleDocIdRDDGroupedByKey =  allShingleDocIdDistinctRDD.groupByKey();
        
/*        JavaPairRDD<String, Integer> allShingleDocIdPairRDD = docIdCommentPairRDD
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, SortedSet<String>>, String, Integer>() {
                  @Override
                  public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, SortedSet<String>> s) {
                    //int urlCount = Iterables.size(s._1);
                    List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
                    for (String n : (String [])s._2.toArray()) {
                      results.add(new Tuple2<String, Integer>(n, s._1));
                    }
                    return results;
                  }
              });*/
        
        //System.out.println("allShingleDocIdPairRDD count: " + allShingleDocIdDistinctRDD.count());
        //allShingleDocIdDistinctRDD.saveAsTextFile("C:\\bigData\\HadoopFreeSpark\\output\\allShingleDocIdDistinctPairRDD");

        System.out.println("allShingleDocIdDistinctRDDGroupedByKey count: " + allShingleDocIdRDDGroupedByKey.count());
        allShingleDocIdRDDGroupedByKey.saveAsTextFile("C:\\bigData\\HadoopFreeSpark\\output\\allShingleDocIdDistinctRDDGroupedByKey");

        //Define the set of Arrays that hold minHash signatures. 
        //JavaRDD<ArrayList<Long>> minHashRDD = distinctAllShingles.map(new Function<String, ArrayList<Long>>() {
        JavaRDD<ArrayList<Long>> minHashRDD = allShingleDocIdRDDGroupedByKey.map(new Function<Tuple2<String,Iterable<Integer>>, ArrayList<Long>>() {
            @Override
            public ArrayList<Long> call(Tuple2<String,Iterable<Integer>> s) {
                ArrayList<Long> longArrayx‬;
                longArrayx‬ = new ArrayList<Long>();
            	for(int i = 0; i < minHashSigLen; i++)
            		longArrayx‬.add(1L<<42);
			return longArrayx‬;
            }
          });
        
        
        JavaPairRDD<String, ArrayList<Long>> minHashPairRDD = allShingleDocIdRDDGroupedByKey.mapToPair(
        		new PairFunction<
        		        Tuple2<String,Iterable<Integer>>, // T as input
        		        String, // as output
        		        ArrayList<Long> // as output
        		>() {
        			@Override
        			public Tuple2<String, ArrayList<Long>> call(Tuple2<String,Iterable<Integer>> element) {
        				ArrayList<Long> longArrayx‬;
                        longArrayx‬ = new ArrayList<Long>();
                    	for(int i = 0; i < minHashSigLen; i++)
                    		longArrayx‬.add(1L<<42);
        				return new Tuple2<String, ArrayList<Long>>(element._1, longArrayx‬);
        			}
        });
        
        
        
        minHashPairRDD.saveAsTextFile("C:\\bigData\\HadoopFreeSpark\\output\\minHashNumbers");

        
        System.out.println("\n\n\n\n\n ");

        sc.close();
    }
}
