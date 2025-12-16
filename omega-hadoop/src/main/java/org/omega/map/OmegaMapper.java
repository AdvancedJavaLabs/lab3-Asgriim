package org.omega.map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OmegaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text category = new Text();
    private final DoubleWritable price = new DoubleWritable();
    private static final String CSV_HEADER = "transaction_id,product_id,category,price,quantity";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith(CSV_HEADER)) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Invalid params in csv file");
        }

        String category = fields[2];
        double price = Double.parseDouble(fields[3]);
        long quantity = Long.parseLong(fields[4]);

        double revenue = price * quantity;

        this.category.set(category);
        this.price.set(revenue);
        context.write(this.category, this.price);
    }

}