package com.guba.test.gateway.hadoop;

import com.guba.test.gateway.Evaluator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Xiangxing Liu
 * @date 2019/11/12
 */
public class GatewayRunner extends Configured implements Tool {
    /**
     * Mapper Class
     */
    private static class GatewayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() != 0){
                String[] line = value.toString().split(",");
                String elapsed = line[1];
                String responseCode = line[3];
                String success = line[7];
                String latency = line[14];
                String idleTime = line[15];
                String connect = line[16];

                if("true".equals(success)) {
                    context.write(new Text("xx_success"), new IntWritable(1));
                }

                if("false".equals(success)) {
                    context.write(new Text("xx_failed"), new IntWritable(1));
                }

                context.write(new Text("xx_total"), new IntWritable(1));
                context.write(new Text("elapsed_avg"), new IntWritable(Integer.parseInt(elapsed)));
                context.write(new Text("yy_responseCode_" + responseCode), new IntWritable(1));
                context.write(new Text("latency_avg"), new IntWritable(Integer.parseInt(latency)));
                context.write(new Text("idleTime_avg"), new IntWritable(Integer.parseInt(idleTime)));
                context.write(new Text("connect_avg"), new IntWritable(Integer.parseInt(connect)));

                context.write(new Text("zz_" + responseCode + "_elapsed_avg"), new IntWritable(Integer.parseInt(elapsed)));
                context.write(new Text("zz_" + responseCode + "_latency_avg"), new IntWritable(Integer.parseInt(latency)));
            }
        }
    }

    /**
     * Reducer Class
     */
    private static class GatewayReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long counter = 0;

            for(IntWritable val : values) {
                if(check(key.toString())) {
                    sum += 1;
                } else {
                    sum += val.get();
                }

                counter += 1;
            }

            if(check(key.toString())) {
                result.set(sum);
            } else {
                result.set(sum / counter);
            }

            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // create job
        Configuration conf = this.getConf();
        Job job = new Job(conf);
        job.setJobName("API_Gateway_Evaluator");
        job.setJarByClass(Evaluator.class);

        // setup MapReduce job
        job.setMapperClass(GatewayMapper.class);
        job.setReducerClass(GatewayReducer.class);

        // setup KV pairs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // setup input and output
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static boolean check(String str) {
        if(str.contains("xx")){
            return true;
        }
        if(str.contains("yy")) {
            return true;
        }
        return false;
    }
}
