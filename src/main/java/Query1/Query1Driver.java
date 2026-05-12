package Query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query1: Tasa de distribución de severidad de anemia por provincia.
 * Job1 cuenta casos por (PROVINCIA, GRADO_SEVERIDAD).
 * Job2 calcula el porcentaje de cada grado de severidad respecto al total provincial.
 */
public class Query1Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Query1Driver <input> <intermediate> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ── Job 1: Contar casos por provincia × severidad ──────────────────
        Job job1 = Job.getInstance(conf, "Q1-Job1: Conteo Provincia-Severidad");
        job1.setJarByClass(Query1Driver.class);
        job1.setMapperClass(Query1Mapper1.class);
        job1.setReducerClass(Query1Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 falló.");
            System.exit(1);
        }

        // ── Job 2: Calcular porcentajes de severidad por provincia ──────────
        Job job2 = Job.getInstance(conf, "Q1-Job2: Tasa de Severidad por Provincia");
        job2.setJarByClass(Query1Driver.class);
        job2.setMapperClass(Query1Mapper2.class);
        job2.setReducerClass(Query1Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
