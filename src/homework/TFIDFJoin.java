package homework;

import homework.types.TermDocumentPair;
import homework.types.TypedRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.DelegatingInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class TFIDFJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TFIDFJoin(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: <tf score path> <idf score path> <output path>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "TFIDFJoin (2015-18525");

        // input & mapper
        job.setInputFormatClass(DelegatingInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TFMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TypedRecord.class);

        // reducer
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(TermDocumentPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : -1;
    }



    public static class TFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
        // fill your code here!
        private Text keyOut = new Text();
        private TypedRecord typedRecord = new TypedRecord();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split line by a space character
            String _line = value.toString();
            String[] lines = _line.split("\n");

            for (String line : lines) {

                String[] temp = line.split("\t");
                int docId = Integer.parseInt(temp[1]);
                String name = new String(temp[0]);
                double score = Double.parseDouble(temp[2]);
                

                typedRecord.setTFScore(docId, score);
                keyOut.set(name);
                context.write(keyOut, typedRecord); 
                
                // System.out.println(docId+" "+name+" "+score);
               
                
            }
        }

    }

    public static class IDFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
        // fill your code here!
       private Text keyOut = new Text();
        private TypedRecord typedRecord = new TypedRecord();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split line by a space character
            String _line = value.toString();
            String[] lines = _line.split("\n");

            for (String line : lines) {

                String[] temp = line.split("\t");
                String name = new String(temp[0]);
                double score = Double.parseDouble(temp[1]);

                //System.out.println(name+" "+score);

                typedRecord.setIDFScore(score);
                keyOut.set(name);
                context.write(keyOut, typedRecord); 

            }
        }
    }

    public static class Node{
        String name;
        double IDFscore;
        ArrayList<TypedRecord> list;

        Node(String _name){
            name = new String(_name);
            list = new ArrayList<TypedRecord>();
        }
    }

    public static class TFIDFReducer extends Reducer<Text, TypedRecord, TermDocumentPair, DoubleWritable> {
        // fill your code here!
        private TermDocumentPair pair = new TermDocumentPair();
        private DoubleWritable valueOut = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<TypedRecord> values, Context context) throws IOException, InterruptedException {
            Node node = new Node(key.toString());

            for (TypedRecord record : values) {
                //System.out.println(record.getType()==TypedRecord.RecordType.TF);
                if(record.getType()==TypedRecord.RecordType.TF){
                    int k = 0;
                    while(k<node.list.size() && node.list.get(k).getScore() > record.getScore()){
                        k++;
                    }
                    node.list.add(k, new TypedRecord(record));
                }
                else{
                    node.IDFscore = record.getScore();
                }
            }

            int n = 10;
            if(node.list.size()<n)
                n = node.list.size();
            
            double score = node.IDFscore;

            for(int i=0; i<n; i++){
                pair.set(new String(node.name), node.list.get(i).getDocumentId());
                valueOut.set(node.list.get(i).getScore()*score);
                context.write(pair, valueOut);
            }

        }
    }
}

