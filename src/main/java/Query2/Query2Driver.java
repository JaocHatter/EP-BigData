package Query2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query2: Edad promedio de diagnóstico (en meses y años) por género y grado de severidad.
 * Job1 agrega la suma de edades normalizadas a meses y cuenta registros.
 * Job2 calcula el promedio final por grupo (GENERO × SEVERIDAD).
 */
public class Query2Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Query2Driver <input> <intermediate> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ── Job 1: Acumular suma de edades y conteo ─────────────────────────
        Job job1 = Job.getInstance(conf, "Q2-Job1: Suma Edades por Genero-Severidad");
        job1.setJarByClass(Query2Driver.class);
        job1.setMapperClass(Query2Mapper1.class);
        job1.setReducerClass(Query2Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 falló.");
            System.exit(1);
        }

        // ── Job 2: Calcular promedio final ──────────────────────────────────
        Job job2 = Job.getInstance(conf, "Q2-Job2: Edad Promedio por Genero y Severidad");
        job2.setJarByClass(Query2Driver.class);
        job2.setMapperClass(Query2Mapper2.class);
        job2.setReducerClass(Query2Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
