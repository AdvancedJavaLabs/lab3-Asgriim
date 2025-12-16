package org.omega.map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OmegaReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context) throws IOException, InterruptedException {

        double sum = 0;
        long count = 0;
        for (DoubleWritable v : values) {
            sum += v.get();
            ++count;
        }

        String output = String.format("%.5f, %d, %.5f", sum, count, sum / count );
        context.write(key, new Text(output));
    }
}