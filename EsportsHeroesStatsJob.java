package lab2.example;

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

public class EsportsHeroesStatsJob {

    // MAPPER
    public static class MatchStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Pomiń nagłówek pliku CSV
            if (line.startsWith("event_id")) {
                return;
            }

            // Zaawansowany split - dzieli po przecinkach, ale ignoruje te wewnątrz cudzysłowów
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            // Weryfikacja liczby kolumn (zgodnie z plikiem datasource1)
            if (fields.length < 8) {
                return;
            }

            String playerId = fields[1].trim();
            // Wyciągnięcie pola bohaterów i usunięcie okalających cudzysłowów
            String heroesField = fields[3].trim().replaceAll("^\"|\"$", "");
            String[] heroes = heroesField.split(";");

            int kills, deaths;
            try {
                kills = Integer.parseInt(fields[4].trim());
                deaths = Integer.parseInt(fields[5].trim());
            } catch (NumberFormatException e) {
                return; // Pomiń błędne wiersze, jeśli liczby są uszkodzone
            }

            String matchResult = fields[7].trim();

            // Obliczenie wskaźników dla danego meczu
            int win = matchResult.equalsIgnoreCase("Win") ? 1 : 0;
            int draw = matchResult.equalsIgnoreCase("Draw") ? 1 : 0;
            int loss = matchResult.equalsIgnoreCase("Lose") ? 1 : 0;

            // Zwycięstwo z przewagą zabójstw nad śmierciami
            int dominantWin = (win == 1 && kills > deaths) ? 1 : 0;

            // Przygotowanie wartości tekstowej ze statystykami: wins,draws,losses,dominant_wins
            String stats = win + "," + draw + "," + loss + "," + dominantWin;

            // Wyemitowanie pary Klucz-Wartość dla KAŻDEGO użytego bohatera w tym meczu
            for (String hero : heroes) {
                String cleanHero = hero.trim();
                if (!cleanHero.isEmpty()) {
                    // Klucz to: player_id,hero_name
                    outKey.set(playerId + "," + cleanHero);
                    outValue.set(stats);
                    context.write(outKey, outValue);
                }
            }
        }
    }
    // COMBINER
    public static class MatchStatsCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalWins = 0;
            int totalDraws = 0;
            int totalLosses = 0;
            int totalDominantWins = 0;

            // Wstępna agregacja statystyk z Mappera działająca lokalnie
            for (Text val : values) {
                String[] stats = val.toString().split(",");
                if (stats.length == 4) {
                    totalWins += Integer.parseInt(stats[0]);
                    totalDraws += Integer.parseInt(stats[1]);
                    totalLosses += Integer.parseInt(stats[2]);
                    totalDominantWins += Integer.parseInt(stats[3]);
                }
            }

            // Przekazanie wstępnie zsumowanych wyników dalej do Reduktora
            // w formacie: wins,draws,losses,dominant_wins
            String combinedStats = totalWins + "," + totalDraws + "," + totalLosses + "," + totalDominantWins;
            outValue.set(combinedStats);
            context.write(key, outValue);
        }
    }
    // REDUCER
    // Zwraca NullWritable jako klucz i Text jako wartość, aby wygenerować czysty plik CSV na wyjściu
    public static class MatchStatsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalWins = 0;
            int totalDraws = 0;
            int totalLosses = 0;
            int totalDominantWins = 0;

            // Agregacja wszystkich statystyk przypisanych do pary gracz-bohater
            for (Text val : values) {
                String[] stats = val.toString().split(",");
                if (stats.length == 4) {
                    totalWins += Integer.parseInt(stats[0]);
                    totalDraws += Integer.parseInt(stats[1]);
                    totalLosses += Integer.parseInt(stats[2]);
                    totalDominantWins += Integer.parseInt(stats[3]);
                }
            }

            // Konstruowanie ostatecznego rekordu: player_id,hero_name,wins,draws,losses,dominant_wins
            String finalCsvLine = key.toString() + "," + totalWins + "," + totalDraws + "," + totalLosses + "," + totalDominantWins;
            outValue.set(finalCsvLine);

            // Zapis za pomocą NullWritable ignoruje domyślny znak tabulacji Hadoopa
            context.write(NullWritable.get(), outValue);
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Użycie: EsportsHeroesStatsJob <input_dir1> <output_dir3>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Esports Heroes Stats");

        job.setJarByClass(EsportsHeroesStatsJob.class);
        job.setMapperClass(MatchStatsMapper.class);
        job.setCombinerClass(MatchStatsCombiner.class);
        job.setReducerClass(MatchStatsReducer.class);

        // Typy wyrzucane przez Mappera (są inne niż Reducera, więc musimy je zdefiniować)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Ostateczne typy danych pliku wynikowego z Reducera
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}