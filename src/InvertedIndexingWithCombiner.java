import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InvertedIndexing{
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        JobConf conf;
        public void configure(JobConf job){
            this.conf=job;
        }
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            //retrieve # keywords from JobConf
            int argc = Integer.parseInt(conf.get("argc"));
            //retrieve all the keywords from JobConf
            List<String> arr = new ArrayList<String>();
            for(int i=0; i<argc; i++){
                String s1 = conf.get("keyword"+i);
                arr.add(s1);
            }
            //get the current file name
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String filename = ""+fileSplit.getPath().getName();

            String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
                String token= tokenizer.nextToken();
                for(int i =0; i<arr.size(); i++){
                    if(token.contains(arr.get(i))){  
                        Text valueInfo = new Text();
                        valueInfo.set(String.format("%s_%s", filename, one));
                        output.collect(new Text(arr.get(i)), valueInfo);
                        break;
                    }       
                }                         	    
	        }
        }
    }

    public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
       // private static final Log LOG = LogFactory.getLog(Reduce.class);

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{

            int totalCount = 0;   
            String filename="";    
            while(values.hasNext()){
                String file = values.next().toString();//????
                String[] parts = file.split("_");
                filename = parts[0];
                totalCount += Integer.parseInt(parts[1]);          
            }
            
            StringBuilder result = new StringBuilder();
            result.append(filename).append("_").append(totalCount);
             //LOG.info("!!" + result.toString());
            output.collect(key, new Text(result.toString()));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
       // private static final Log LOG = LogFactory.getLog(Reduce.class);

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            java.util.Map<String, Integer> hashResult = new HashMap<String, Integer>();           
            while(values.hasNext()){
                String file = values.next().toString();//????
                String[] parts = file.split("_");
                //LOG.info("!!!input:" +file);
                /*if(parts.length != 2)
                {
                    LOG.info("!!!not valid input:" +file);
                }*/
                String filename = parts[0];
                int count = Integer.parseInt(parts[1]);
                if(hashResult.containsKey(filename)){
                    hashResult.put(filename, hashResult.get(filename)+count);
                }else{
                    hashResult.put(filename,count);
                }             
            }
            List<java.util.Map.Entry<String, Integer>> list = new LinkedList<java.util.Map.Entry<String, Integer>>( hashResult.entrySet() );
            Collections.sort(list, new Comparator<java.util.Map.Entry<String, Integer>>(){
                public int compare(java.util.Map.Entry<String, Integer> o1, java.util.Map.Entry<String, Integer> o2 )
                {
                    return (o1.getValue()).compareTo( o2.getValue() );
                }
            });

            boolean isfirst = true;
            StringBuilder result = new StringBuilder();
            for(java.util.Map.Entry<String, Integer> entry: list){
                if(!isfirst){
                 result.append(" ");
                }
                isfirst = false;
             result.append(entry.getKey()).append("_").append(entry.getValue());
            }
           // LOG.info("!!" + result.toString());
            output.collect(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(InvertedIndexing.class);
        conf.setJobName("invertIndex");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combiner.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //conf.setNumMapTasks(4);
        //conf.setNumReduceTasks(4);
        conf.set("argc", String.valueOf(args.length-2));
        for(int i=0; i<args.length-2; i++){
            conf.set("keyword"+i, args[i+2]);
        }

        long start = new Date().getTime(); 

        JobClient.runJob(conf);

        long end = new Date().getTime(); 
        System.out.println("Job took "+(end-start) + "milliseconds");
    }
}