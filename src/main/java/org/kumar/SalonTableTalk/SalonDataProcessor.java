package org.kumar.SalonTableTalk;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;















import scala.Tuple3;



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
        String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\TEST\\tableTalkComments*.txt";
        String allShingleDocIdDistinctRDDGroupedByKeyFile = "C:\\bigData\\HadoopFreeSpark\\output\\allShingleDocIdDistinctRDDGroupedByKey";
        String minHashNumbersFile = "C:\\bigData\\HadoopFreeSpark\\output\\minHashNumbers";
        String shingleIdMappedToDocIdListsFile = "C:\\bigData\\HadoopFreeSpark\\output\\shingleIdMappedToDocIdLists";
        String docIdMinHashValuePairsFile = "C:\\bigData\\HadoopFreeSpark\\output\\docIdMinHashValuePairs";

        //String dataFile = "input/tableTalkComments*.txt";
        //String allShingleDocIdDistinctRDDGroupedByKeyFile = "output/allShingleDocIdDistinctRDDGroupedByKey";
        //String minHashNumbersFile = "output/minHashNumbers";
    	
    	final int shingleLength=6;
        final int minHashSigLen=25;
        final SalonDataProcessor salonDataProcessor = new SalonDataProcessor();
        
        SparkConf conf = new SparkConf().setAppName("Salon Table Talk Data Processor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> commentsRawData = sc.textFile(dataFile).cache();
        JavaRDD<String> commentsRawData = sc.textFile(dataFile);
    	
        JavaPairRDD<Long, SortedSet<String>> docIdShingleSetPairRDD = commentsRawData.mapToPair(
        		new PairFunction<
        				String, // T as input
        				Long, // K as output
        				SortedSet<String> // V as output
        		>() {
        			@Override
        			public Tuple2<Long, SortedSet<String>> call(String element) {
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
        				return new Tuple2<Long, SortedSet<String>>(new Long(tokens[0]), sortedShingleSet);
        			}
        });
        
        JavaPairRDD<Long, ArrayList<Long>> minHashPairRDD = docIdShingleSetPairRDD.mapToPair(
        		new PairFunction<
        		        Tuple2<Long, SortedSet<String>>, // T as input
        		        Long, // as output
        		        ArrayList<Long> // as output
        		>() {
        			@Override
        			public Tuple2<Long, ArrayList<Long>> call(Tuple2<Long, SortedSet<String>> element) {
        				ArrayList<Long> longArrayx‬ = new ArrayList<Long>();
                    	for(int i = 0; i < minHashSigLen; i++)
                    		longArrayx‬.add(1L<<42);
        				return new Tuple2<Long, ArrayList<Long>>(element._1, longArrayx‬);
        			}
        });
    
        //System.out.println("minHashPairRDD count: " + minHashPairRDD.count());
        //minHashPairRDD.saveAsTextFile(minHashNumbersFile);

        JavaPairRDD<String, Long> allShingleDocIdDistinctRDD = docIdShingleSetPairRDD
        		.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, SortedSet<String>>, String, Long>() {
			@Override
			public Iterable<Tuple2<String, Long>> call(Tuple2<Long, SortedSet<String>> arg0)
					throws Exception {
				List<Tuple2<String, Long>> result = new ArrayList<Tuple2<String, Long>>();
				for (String elt : arg0._2){
					result.add(new Tuple2<String, Long>(elt, arg0._1));
				}
				return result;
			}
          });
    
        JavaPairRDD<String, Iterable<Long>> allShingleDocIdRDDGroupedByKey =  allShingleDocIdDistinctRDD.groupByKey().sortByKey();
        JavaPairRDD<Tuple2<String, Iterable<Long>>, Long> allShingleDocIdRDDGroupedByKeyIndexed =  allShingleDocIdRDDGroupedByKey.zipWithIndex();
        
        JavaPairRDD<Long, Iterable<Long>> shingleIdMappedToDocIdLists =  allShingleDocIdRDDGroupedByKeyIndexed
        							.mapToPair(new PairFunction<Tuple2<Tuple2<String, Iterable<Long>>, Long>, Long, Iterable<Long>>(){
										@Override
										public Tuple2<Long, Iterable<Long>> call(
												Tuple2<Tuple2<String, Iterable<Long>>, Long> arg0)
												throws Exception {
													long shingleId = arg0._2;
													Iterable<Long> docIdsList = arg0._1._2;											
											return (new Tuple2<Long, Iterable<Long>>(shingleId, docIdsList)) ;
										}
        								
        							}					
        		);

        Long numOfShingles = shingleIdMappedToDocIdLists.count();
        System.out.println("shingleIdMappedToDocIdLists count: " + numOfShingles);
        shingleIdMappedToDocIdLists.saveAsTextFile(shingleIdMappedToDocIdListsFile);
        
        HashFunctionUtility hashUtil = new HashFunctionUtility(numOfShingles);
        hashUtil.SelectRandomPrimes(minHashSigLen);
        final Broadcast<HashFunctionUtility> broadCastedHashUtil = sc.broadcast(hashUtil);
        
        //int minHashIndex=0;
        
        JavaPairRDD<Long,Long> docIdMinHashValuePairs = shingleIdMappedToDocIdLists
        			.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long,Long>() {
			@Override
			public Iterable<Tuple2<Long, Long>> call(Tuple2<Long, Iterable<Long>> arg0)
					throws Exception {
				ArrayList<Tuple2<Long,Long>> docIdHashValuePair = new ArrayList<Tuple2<Long,Long>>();
				for (Long docId: arg0._2){
					Long primeForHash = broadCastedHashUtil.getValue().selectedRandomPrimes.get(0);
					Long hashVal = broadCastedHashUtil.getValue().FindHashValue(arg0._1, primeForHash);
					docIdHashValuePair.add(new Tuple2<Long,Long>(docId, hashVal));
				}
				return docIdHashValuePair;
			}
          }).reduceByKey(new Function2<Long,Long,Long>(){
        	  @Override
        	  public Long call(Long l1, Long l2){
        		  return Math.min(l1, l2);
        	  }
          }).sortByKey();
        
        Long docIdMinHashValuePairsCount = docIdMinHashValuePairs.count();
        System.out.println("docIdMinHashValuePairsCount: " + docIdMinHashValuePairsCount);
        docIdMinHashValuePairs.saveAsTextFile(docIdMinHashValuePairsFile);
          

        //System.out.println("allShingleDocIdDistinctRDDGroupedByKey count: " + allShingleDocIdRDDGroupedByKey.count());
        //allShingleDocIdRDDGroupedByKey.saveAsTextFile(allShingleDocIdDistinctRDDGroupedByKeyFile);
    
        /*allShingleDocIdRDDGroupedByKey.foreach(new Function2<String, Iterable<Long>, Void>() {
            @Override
            public Void call(String, Iterable<Long> rdd) throws IOException {
              String counts = "Counts at time " + time + " " + rdd.collect();
              System.out.println(counts);
              System.out.println("Appending to " + outputFile.getAbsolutePath());
              Files.append(counts + "\n", outputFile, Charset.defaultCharset());
              return null;
            }
          });
        
        
        allShingleDocIdRDDGroupedByKey.foreach(new VoidFunction<Tuple2<String, Iterable<Long>>>(){ 
        	@Override
        	public void call(Tuple2<String, Iterable<Long>> asdirgbp) {
        		for(Long colId : asdirgbp._2){
        			for(int i = 0; i < minHashSigLen; i++){
        				//long curr = minHashPairRDD.lookup(colId).get(0).get(i);
        				//long candidate = 250L;
        				// minHashPairRDD.lookup(colId).get(0).set(i, candidate);
        			}
        		}

          }
        });*/
        
        System.out.println("minHashPairRDD count: " + minHashPairRDD.count());
        minHashPairRDD.saveAsTextFile(minHashNumbersFile);
        
        
    }
	
	/*
    public static void main( String[] args )
    {
        String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\TEST\\tableTalkComments*.txt";
        String allShingleDocIdDistinctRDDGroupedByKeyFile = "C:\\bigData\\HadoopFreeSpark\\output\\allShingleDocIdDistinctRDDGroupedByKey";
        String minHashNumbersFile = "C:\\bigData\\HadoopFreeSpark\\output\\minHashNumbers";

        //String dataFile = "input/tableTalkComments*.txt";
        //String allShingleDocIdDistinctRDDGroupedByKeyFile = "output/allShingleDocIdDistinctRDDGroupedByKey";
        //String minHashNumbersFile = "output/minHashNumbers";
    	
    	final int shingleLength=6;
        final int minHashSigLen=25;
        final SalonDataProcessor salonDataProcessor = new SalonDataProcessor();
        
        SparkConf conf = new SparkConf().setAppName("Salon Table Talk Data Processor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> commentsRawData = sc.textFile(dataFile).cache();
        JavaRDD<String> commentsRawData = sc.textFile(dataFile);

        long commentsCount = commentsRawData.count();

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
        
        System.out.println("allShingleDocIdDistinctRDDGroupedByKey count: " + allShingleDocIdRDDGroupedByKey.count());
        allShingleDocIdRDDGroupedByKey.saveAsTextFile(allShingleDocIdDistinctRDDGroupedByKeyFile);


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
        
        //docIdCommentPairRDD
        
        
        minHashPairRDD.saveAsTextFile(minHashNumbersFile);

        
        System.out.println("\n\n\n\n\n ");

        sc.close();
    }
    */
}
