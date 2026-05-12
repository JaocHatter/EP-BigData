package Query4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query4: Árbol de Decisión para clasificar el grado de severidad de anemia.
 * Características: grupo_etario (INF/TOD/YOU/PRE) × género (M/F).
 *
 * Job1 construye la distribución de clases por hoja (feature_combo → distribución).
 *      También calcula la impureza Gini de cada hoja.
 * Job2 evalúa las reglas: determina la clase mayoritaria por hoja,
 *      calcula exactitud de entrenamiento y ganancia de Gini del árbol.
 */
public class Query4Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Query4Driver <input> <intermediate> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ── Job 1: Contar distribución de clases por (grupo_etario × género) ─
        Job job1 = Job.getInstance(conf, "Q4-Job1: Distribucion de Clases por Hoja DT");
        job1.setJarByClass(Query4Driver.class);
        job1.setMapperClass(Query4Mapper1.class);
        job1.setReducerClass(Query4Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 falló.");
            System.exit(1);
        }

        // ── Job 2: Generar reglas y calcular métricas del árbol ─────────────
        Job job2 = Job.getInstance(conf, "Q4-Job2: Reglas y Exactitud del Arbol de Decision");
        job2.setJarByClass(Query4Driver.class);
        job2.setMapperClass(Query4Mapper2.class);
        job2.setReducerClass(Query4Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
