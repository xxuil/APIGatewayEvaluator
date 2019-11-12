package com.guba.test.gateway.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Mapreduce {

    private static class GatewayMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    }

    private static class GatewayReducer extends Reducer<NullWritable, NullWritable, NullWritable, NullWritable> {

    }
}
