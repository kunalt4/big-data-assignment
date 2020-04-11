import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.DayOfWeek;
import java.lang.Exception;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTripDistance_Q2 {

    public static class AverageTripDistanceMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final static FloatWritable one = new FloatWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            word.set(value);
            String nextLine = word.toString();
            String[] columns = nextLine.split(",");

            Text distanceKey = new Text("Total_Distance");

            Text tripKey = new Text("Total_Trip");

            if (!columns[0].trim().isEmpty()) {
                // formatter for datetime
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                try {
                    // datetime column converted to datetime
                    LocalDateTime date = LocalDateTime.parse(columns[1], formatter);
                    System.out.println(date);

                    // map distancekey to distance
                    context.write(distanceKey, new FloatWritable(Float.parseFloat(columns[4])));

                    // map tripKey to 1 to find total trips
                    context.write(tripKey, one);

                    // map days_Distance to distance to find total distance in the
                    // day
                    // THURSDAY_Distance
                    DayOfWeek dayOfWeek = date.getDayOfWeek();

                    Text daysKey = new Text(dayOfWeek + "_Distance");
                    context.write(daysKey, new FloatWritable(Float.parseFloat(columns[4])));

                    // map day_Trip to 1 to find total trips in the day
                    context.write(new Text(dayOfWeek + "_Trip"), one);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Get intermediate trip distances for each key and intermediate trip count.
    // This is done at each split.
    // Combiner used to summarize mapped records with the same key
    public static class AverageTripDistanceCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float count = 0;
            for (FloatWritable value : values) {
                count += value.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static class AverageTripDistanceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();
        float totTripDistance = 0;

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            // If "Trip" is present in the key, calculate the total number of trips, the
            // average and write it to output
            if (key.find("Trip") > 0) {
                for (FloatWritable val : values) {
                    sum += val.get();
                }

                // else calculate total trip distance

                String[] pkey = key.toString().split("_");
                context.write(new Text(pkey[0] + "_TripDistance_Average"), new FloatWritable(totTripDistance / sum));

            }

            else {

                for (FloatWritable val : values) {
                    sum += val.get();
                }
                totTripDistance = sum;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average trip distance");
        job.setJarByClass(AvgTripDistance_Q2.class);
        job.setMapperClass(AverageTripDistanceMapper.class);
        job.setCombinerClass(AverageTripDistanceCombiner.class);
        job.setReducerClass(AverageTripDistanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
