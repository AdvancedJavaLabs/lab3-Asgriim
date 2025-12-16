package org.omega;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omega.map.OmegaMapper;
import org.omega.map.OmegaReducer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar <jar-file> <input> <output>");
            System.err.println("Example: hadoop jar omega-hadoop.jar /input /output/test");
            System.exit(1);
        }

        String input = otherArgs[0];
        String output = otherArgs[1];

        int bufSize = conf.getInt("omegaBufSz", 128 * 1024);
        int mapMemory = conf.getInt("omegaMapSz", 4 * 1024);
        int reduceMemory = conf.getInt("omegaReduceSz", 4 * 1024);
        long dfsBlockSz = conf.getLong("omegaDfsBlockSz", 1048576);
        boolean compress = conf.getBoolean("omegaCompress", false);
        int reducersCount = conf.getInt("omegaReducersCount", 1);

        conf.setInt("mapreduce.map.memory.mb", mapMemory);
        conf.setInt("mapreduce.reduce.memory.mb", reduceMemory);
        conf.setLong("dfs.blocksize", dfsBlockSz);
        conf.setInt("io.file.buffer.size", bufSize);
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.setInt("mapreduce.job.reduces", reducersCount);

        if (compress) {
            conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.map.output.compress.codec",
                    "org.apache.hadoop.io.compress.DefaultCodec");
        }

        System.out.println("Reading " + input + " to " + output + " with size " + bufSize);
        System.out.println("Reading " + input + " to " + output + " with map size " + mapMemory);
        System.out.println("Reading " + input + " to " + output + " with reduce size " + reduceMemory);
        System.out.println("Reading " + input + " to " + output + " with dfs size " + dfsBlockSz);
        System.out.println("Reading " + input + " to " + output + " with compress " + compress);

        Job job = Job.getInstance(conf, "Transactions Processing " );
        job.setJarByClass(Main.class);

        job.setMapperClass(OmegaMapper.class);
        job.setReducerClass(OmegaReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        System.out.println("Job finished in " + (endTime - startTime) + "ms");
        System.out.println("success status: " + success);
        System.exit(success ? 0 : 1);
    }
}