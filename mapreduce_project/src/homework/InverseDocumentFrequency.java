package homework;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Math.*;

public class InverseDocumentFrequency extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InverseDocumentFrequency(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: <input path> <output path> <number of documents>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "InverseDocumentFrequency (2015-18525)");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
        job.setMapperClass(IDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer
        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // passing number of documents
        job.getConfiguration().setInt("totalDocuments", Integer.parseInt(args[2]));

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
   //////////////////////////////////////////////////////////////////////


    public static class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // fill your code here!
        private Text keyOut = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split line by a space character
            String _line = value.toString();
            String[] lines = _line.split("\n");

            for (String line : lines) {

                String[] temp = line.split("\t");

                String[] words = temp[1].split(" ");   

                //Build a Hash Table

                Hash hash = new Hash(109);

                for(String word : words){             
                    hash.insert(word);
                }

                //Read from the Hash

                for(int i=0; i<hash.size; i++){
                    
                    Node point = hash.hash[i].next;
                    //System.out.println(i);
                    while(point!=null){
                        
                        keyOut.set(point.value);
                        context.write(keyOut, one); 
                        point = point.next;
                    }
                    
                }
            }
        }
    }

    public static class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        // you may assume that this variable contains the total number of documents.
        private int totalDocuments;
        private DoubleWritable valueOut = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocuments = context.getConfiguration().getInt("totalDocuments", 1);
        }


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // sum the all received counts
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            valueOut.set(-Math.log((sum+0.0)/totalDocuments));

            // write a (word, count) pair to output file. The word and count are separated by a tab character.
            context.write(key, valueOut);
        }

        // fill your code here!
    }
}

