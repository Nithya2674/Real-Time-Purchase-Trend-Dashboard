package org.bda;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

/**
 * Enterprise Hadoop MapReduce Job for Trailing 60-Minute Top 20 Instacart Products.
 * 
 * Usage:
 *   hadoop jar top20-mapreduce-1.0.jar org.bda.Top20ProductsMR \
 *     /data/instacart_transactions \
 *     /output/trailing_60min
 */
public class Top20ProductsMR {

    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text productKey = new Text();
        private Text productVal = new Text();
        private long cutoffEpoch;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            int windowMinutes = conf.getInt("trailing.window.minutes", 60);
            long nowEpoch = System.currentTimeMillis() / 1000L;
            this.cutoffEpoch = nowEpoch - (windowMinutes * 60L);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            try {
                JSONObject json = new JSONObject(line);
                long epoch = json.optLong("epoch_seconds", 0L);

                // Trailing 60-minute window filter
                if (epoch >= cutoffEpoch) {
                    int productId = json.getInt("product_id");
                    int quantity = json.optInt("quantity", 1);
                    String name = json.optString("product_name", "Unknown");
                    String dept = json.optString("department", "other");
                    double totalPrice = json.optDouble("total_price", 0.0);

                    productKey.set(String.valueOf(productId));
                    // Value payload: quantity,name,department,totalPrice
                    productVal.set(quantity + "\t" + name + "\t" + dept + "\t" + totalPrice);
                    context.write(productKey, productVal);
                }
            } catch (Exception e) {
                // Ignore malformed JSON records
            }
        }
    }

    public static class Top20Reducer extends Reducer<Text, Text, Text, Text> {
        private static class ProductStat {
            String productId;
            String name;
            String department;
            int totalUnits;
            double totalRevenue;

            ProductStat(String id, String name, String dept, int units, double rev) {
                this.productId = id;
                this.name = name;
                this.department = dept;
                this.totalUnits = units;
                this.totalRevenue = rev;
            }
        }

        private PriorityQueue<ProductStat> minHeap;
        private final int TOP_K = 20;

        @Override
        protected void setup(Context context) {
            // Min-heap ordered by totalUnits ascending to retain top 20
            minHeap = new PriorityQueue<>(TOP_K, Comparator.comparingInt(a -> a.totalUnits));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {
            int sumUnits = 0;
            double sumRev = 0.0;
            String pName = "";
            String pDept = "";

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length >= 4) {
                    sumUnits += Integer.parseInt(parts[0]);
                    pName = parts[1];
                    pDept = parts[2];
                    sumRev += Double.parseDouble(parts[3]);
                }
            }

            ProductStat stat = new ProductStat(key.toString(), pName, pDept, sumUnits, sumRev);
            minHeap.offer(stat);
            if (minHeap.size() > TOP_K) {
                minHeap.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<ProductStat> topList = new ArrayList<>(minHeap);
            // Sort descending
            topList.sort((a, b) -> Integer.compare(b.totalUnits, a.totalUnits));

            int rank = 1;
            for (ProductStat p : topList) {
                String outputValue = String.format("Rank: #%d | %s | Dept: %s | Units Sold: %d | Revenue: $%.2f",
                        rank++, p.name, p.department, p.totalUnits, p.totalRevenue);
                context.write(new Text(p.productId), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: Top20ProductsMR <input_hdfs_dir> <output_hdfs_dir> [window_minutes]");
            System.exit(2);
        }

        if (args.length >= 3) {
            conf.setInt("trailing.window.minutes", Integer.parseInt(args[2]));
        }

        Job job = Job.getInstance(conf, "Instacart Trailing 60-Min Top 20 Products");
        job.setJarByClass(Top20ProductsMR.class);
        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(Top20Reducer.class);
        job.setNumReduceTasks(1); // Single reducer to consolidate Top 20

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
