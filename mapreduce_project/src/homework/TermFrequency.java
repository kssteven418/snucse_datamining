package homework;

import homework.types.TermDocumentPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TermFrequency extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TermFrequency(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <input path> <output path>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "TermFrequency (2015-18525)");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
        job.setMapperClass(TFMapper.class);
        job.setMapOutputKeyClass(TermDocumentPair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // reducer
        job.setNumReduceTasks(0); // we use only mapper for this MapReduce job!
        job.setOutputKeyClass(TermDocumentPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }


///////////////////////////////HASHED STRUCTURE DEFINITION////////////////////////////////////
    public static class Node{
        
        int num;
        String value;
        Node next;

        public Node(){
            next = null;
            num = -1;
            value = "";
        }

        public Node(int n){
            next = null;
            num = n;
            value = "";
        }
    }

    public static class Hash{

        int size;
        Node[] hash;

        Hash(int _size){
            size = _size;
            hash = new Node[size];
            for(int i=0; i<size; i++){
                hash[i] = new Node();
            }
        }

        //Hash Ftn : map a String value to a integer index
        int hashFtn(String value){
            int sum = 0;
            for(int i=0; i<value.length(); i++){
                sum = sum*('z'-'a'+1);
                sum += (value.charAt(i)-'a');
            }
            sum = sum%size;

            if(sum<0)
                sum += size;
            return sum;
        }


        void insert(String value){
            int key = hashFtn(value);
            Node point = hash[key];
            while(true){

                if(point.value.equals(value)){
                    point.num++;
                    break;
                }

                if(point.next==null){                   
                    Node newNode = new Node(1);
                    newNode.value = new String(value);
                    point.next = newNode;
                    break;
                }
                
                point = point.next;
            }
        }
    }
    //////////////////////////////////////////////////////////////////////////////  

    
    public static class TFMapper extends Mapper<LongWritable, Text, TermDocumentPair, DoubleWritable> {
        // fill your code here!
        private TermDocumentPair keyOut = new TermDocumentPair();
        private DoubleWritable doubleW = new DoubleWritable(1); 

        

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split line by a space character
            String _line = value.toString();
            String[] lines = _line.split("\n");

            for (String line : lines) {

                String[] temp = line.split("\t");
                int docId = Integer.parseInt(temp[0]);

                String[] words = temp[1].split(" ");

                Hash hash = new Hash(109);

                //Build Hash Table

                for(String word : words){             
                    hash.insert(word);

                }

                //Read from Hash

                int maxnum = 0;

                for(int i=0; i<hash.size; i++){
                    
                    Node point = hash.hash[i].next;
                    
                    while(point!=null){
                        
                        if(point.num>maxnum)
                            maxnum = point.num;

                        point = point.next;
                    }
                    
                }

                for(int i=0; i<hash.size; i++){
                    
                    Node point = hash.hash[i].next;
                    
                    while(point!=null){
                        
                        keyOut.set(point.value, docId);
                        doubleW = new DoubleWritable(point.num*0.5/maxnum+0.5);
                        context.write(keyOut, doubleW); 
                        point = point.next;
                    }
                    
                }
                
            }
        }
    }
}

