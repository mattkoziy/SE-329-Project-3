package sparkwordcount;

import java.util.ArrayList;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;

public class JavaWordCount {

    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        String goal = args[2];
        
        if( !(goal.equals("gain")) && !(goal.equals("lose")) ){
        	System.out.println("Must pass in either 'gain' or 'lose' argument into the program");
        	System.exit(0);
        }
        
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our nutritional input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // map/split each line to multiple data segments
        JavaRDD<String> data = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                }
        );

      
   
        //calculate user's current averages for carbs, fats, and proteins
        ArrayList<Integer> carbs = new ArrayList<Integer>();
        ArrayList<Integer> fats = new ArrayList<Integer>();
        ArrayList<Integer> proteins = new ArrayList<Integer>();
        
        for(int i=0; i < data.count(); i++){
        	//if the item in the array is a carb
        	if(i % 3 == 0){
        		carbs.add(Integer.parseInt(data.toArray().get(i)));
        	}
        	//if the item in the array is a fat
        	if( (i+2) % 3 == 0){
        		fats.add(Integer.parseInt(data.toArray().get(i)));
        	}
        	//if the item in the array is a protein
        	if( (i+1) % 3 == 0){
        		proteins.add(Integer.parseInt(data.toArray().get(i)));
        	}
        }
        
        
        //get current averages based on grams of carbs, fats, and proteins and transform into a percentage
        int currentCarbsSum = 0;
        for(int i=0; i < carbs.size(); i++){
        	currentCarbsSum += carbs.get(i);
        }
        String currentCarbsGrams = Integer.toString(currentCarbsSum / carbs.size()); 
        
        int currentFatsSum = 0;
        for(int i=0; i < fats.size(); i++){
        	currentFatsSum += fats.get(i);
        }
        String currentFatsGrams = Integer.toString(currentFatsSum / fats.size()); 
        
        int currentProteinsSum = 0;
        for(int i=0; i < proteins.size(); i++){
        	currentProteinsSum += proteins.get(i);
        }
        String currentProteinsGrams = Integer.toString(currentProteinsSum / proteins.size()); 
        
        
        System.out.println("Your current carb intake is " + currentCarbsGrams + " grams");
        System.out.println("Your current fat intake is " + currentFatsGrams + " grams");
        System.out.println("Your current protein intake is " + currentProteinsGrams + " grams");
        System.out.println();
        
        
        
        String carbsPrediction = null;
        String fatsPrediction = null;
        String proteinsPrediction = null;
        //calculate how to improve the users health based on either "gain" or "lose" input passed into the program
        if(goal.equals("gain")){
        	carbsPrediction = Double.toString((Double.parseDouble(currentCarbsGrams) * 1.2));
        	fatsPrediction = Double.toString((Double.parseDouble(currentFatsGrams) * 1.2));
        	proteinsPrediction = Double.toString((Double.parseDouble(currentProteinsGrams) * 1.2));
        }
        if(goal.equals("lose")){
        	carbsPrediction = Double.toString((Double.parseDouble(currentCarbsGrams) * 0.8));
        	fatsPrediction = Double.toString((Double.parseDouble(currentFatsGrams) * 0.8));
        	proteinsPrediction = Double.toString((Double.parseDouble(currentProteinsGrams) * 0.8));
        }
        
        System.out.println("In order to "+ goal + " weight");
        System.out.println("Your carb intake should be " + carbsPrediction);
        System.out.println("Your fat intake should be " + fatsPrediction);
        System.out.println("Your protein intake should be " + proteinsPrediction);
        
        //save the data to the output file
        data.saveAsTextFile(outputFile);
        
        

    }
    
    
}
