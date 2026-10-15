import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Weekly return rate per product category.
 *
 * Input:  the JSON events the Kafka consumer stored in HDFS, one per line:
 *         {"event_type": "ORDER" | "RETURN_REQUEST", "category": "...", "event_date": "YYYY-MM-DD"}
 * Output: week  category  orders  returns  return_rate  FLAGGED|OK   (tab separated)
 *         A category is FLAGGED when more than 10% of its orders that week were returned.
 */
public class ReturnRateJob {

    static final double THRESHOLD = 0.10;

    /** Emits ("2024-W05|Electronics", "1,0") for an order and ("2024-W05|Electronics", "0,1") for a return request. */
    public static class EventMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text ORDER = new Text("1,0");
        private static final Text RETURN = new Text("0,1");
        private final ObjectMapper json = new ObjectMapper();
        private final Text weekCategory = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String type, category;
            LocalDate date;
            try {
                JsonNode event = json.readTree(line.toString());
                type = event.path("event_type").asText();
                category = event.path("category").asText();
                date = LocalDate.parse(event.path("event_date").asText());
            } catch (Exception e) {
                context.getCounter("ReturnRate", "BAD_EVENTS").increment(1);   // skip malformed lines
                return;
            }
            String week = String.format(Locale.ROOT, "%d-W%02d",
                    date.get(IsoFields.WEEK_BASED_YEAR), date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
            weekCategory.set(week + "|" + category);

            if ("ORDER".equals(type)) {
                context.write(weekCategory, ORDER);
            } else if ("RETURN_REQUEST".equals(type)) {
                context.write(weekCategory, RETURN);
            }
        }
    }

    /** Combiner: adds up "orders,returns" pairs on the map side so far less data is shuffled. */
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long[] total = sum(values);
            context.write(key, new Text(total[0] + "," + total[1]));
        }
    }

    /** Reducer: totals each week/category, computes returns / orders and flags rates above 10%. */
    public static class RateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long[] total = sum(values);
            long orders = total[0], returns = total[1];
            double rate = orders == 0 ? 0.0 : (double) returns / orders;
            String[] weekAndCategory = key.toString().split("\\|", 2);

            context.write(new Text(weekAndCategory[0]), new Text(String.format(Locale.ROOT, "%s\t%d\t%d\t%.4f\t%s",
                    weekAndCategory[1], orders, returns, rate, rate > THRESHOLD ? "FLAGGED" : "OK")));
        }
    }

    static long[] sum(Iterable<Text> values) {
        long orders = 0, returns = 0;
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            orders += Long.parseLong(parts[0]);
            returns += Long.parseLong(parts[1]);
        }
        return new long[] {orders, returns};
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReturnRateJob <input dir> <output dir>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "weekly-return-rate");
        job.setJarByClass(ReturnRateJob.class);
        job.setMapperClass(EventMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(RateReducer.class);
        job.setNumReduceTasks(1);                       // one small result file for the dashboard
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // The consumer writes many small files; pack them into 128 MB splits instead of one map task per file.
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 128L * 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
