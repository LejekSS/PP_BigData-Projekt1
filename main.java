package bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class main {

    public static class MatchStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("event_id")) {
                return;
            }

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (fields.length < 8) {
                return;
            }
            String playerId = fields[1].trim();
            String heroesField = fields[3].trim().replaceAll("^\"|\"$", "");
            String[] heroes = heroesField.split(";");

            int kills, deaths;
            try {
                kills = Integer.parseInt(fields[4].trim());
                deaths = Integer.parseInt(fields[5].trim());
            } catch (NumberFormatException e) {
                return;
            }

            String matchResult = fields[7].trim();
            int win = matchResult.equalsIgnoreCase("Win") ? 1 : 0;
            int draw = matchResult.equalsIgnoreCase("Draw") ? 1 : 0;
            int loss = matchResult.equalsIgnoreCase("Lose") ? 1 : 0;
            int dominantWin = (win == 1 && kills > deaths) ? 1 : 0;
            String stats = win + "," + draw + "," + loss + "," + dominantWin;

            for (String hero : heroes) {
                String cleanHero = hero.trim();
                if (!cleanHero.isEmpty()) {
                    outKey.set(playerId + "," + cleanHero);
                    outValue.set(stats);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class MatchStatsCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalWins = 0;
            int totalDraws = 0;
            int totalLosses = 0;
            int totalDominantWins = 0;

            for (Text val : values) {
                String[] stats = val.toString().split(",");
                if (stats.length == 4) {
                    totalWins += Integer.parseInt(stats[0]);
                    totalDraws += Integer.parseInt(stats[1]);
                    totalLosses += Integer.parseInt(stats[2]);
                    totalDominantWins += Integer.parseInt(stats[3]);
                }
            }

            String combinedStats = totalWins + "," + totalDraws + "," + totalLosses + "," + totalDominantWins;
            outValue.set(combinedStats);
            context.write(key, outValue);
        }
    }

    public static class MatchStatsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalWins = 0;
            int totalDraws = 0;
            int totalLosses = 0;
            int totalDominantWins = 0;

            for (Text val : values) {
                String[] stats = val.toString().split(",");
                if (stats.length == 4) {
                    totalWins += Integer.parseInt(stats[0]);
                    totalDraws += Integer.parseInt(stats[1]);
                    totalLosses += Integer.parseInt(stats[2]);
                    totalDominantWins += Integer.parseInt(stats[3]);
                }
            }

            String finalCsvLine = key.toString() + "," + totalWins + "," + totalDraws + "," + totalLosses + "," + totalDominantWins;
            outValue.set(finalCsvLine);
            context.write(NullWritable.get(), outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BigData_Projekt1_MapReduce");

        job.setJarByClass(main.class);
        job.setMapperClass(MatchStatsMapper.class);
        job.setCombinerClass(MatchStatsCombiner.class);
        job.setReducerClass(MatchStatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
