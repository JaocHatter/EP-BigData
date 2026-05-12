package Query3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query3: Índice de carga de severidad por Red de Salud.
 * Pondera LEV=1.0, MOD=2.0, SEV=3.0 para obtener un índice compuesto por red.
 * Job1 acumula casos ponderados y totales por (RED × SEVERIDAD).
 * Job2 calcula el índice final: suma_ponderada / total_casos por RED.
 */
public class Query3Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Query3Driver <input> <intermediate> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ── Job 1: Acumular pesos y casos por RED × SEVERIDAD ───────────────
        Job job1 = Job.getInstance(conf, "Q3-Job1: Casos por Red-Severidad");
        job1.setJarByClass(Query3Driver.class);
        job1.setMapperClass(Query3Mapper1.class);
        job1.setReducerClass(Query3Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 falló.");
            System.exit(1);
        }

        // ── Job 2: Calcular índice ponderado por RED ────────────────────────
        Job job2 = Job.getInstance(conf, "Q3-Job2: Indice de Carga de Severidad por Red");
        job2.setJarByClass(Query3Driver.class);
        job2.setMapperClass(Query3Mapper2.class);
        job2.setReducerClass(Query3Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
