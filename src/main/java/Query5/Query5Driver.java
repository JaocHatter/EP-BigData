package Query5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Query5: Perceptrón (red neuronal de una capa) con gradiente descendiente.
 * Clasifica si el caso es SEVERO (1) vs no severo (0).
 * Características normalizadas: edad_meses/60, género (M=0/F=1), tipo_diag (D=1/R=0.5/P=0).
 *
 * Job1 ejecuta una época de gradiente descendiente: forward-pass + backward-pass
 *      y produce los pesos actualizados + pérdida promedio.
 * Job2 carga los nuevos pesos desde la configuración, clasifica todo el dataset
 *      y calcula exactitud, precisión y recall.
 */
public class Query5Driver {

    // Pesos iniciales del perceptrón
    public static final double INIT_W1   = 0.1;
    public static final double INIT_W2   = 0.1;
    public static final double INIT_W3   = 0.1;
    public static final double INIT_BIAS = 0.0;
    public static final double LR        = 0.01;  // tasa de aprendizaje

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: Query5Driver <input> <intermediate> <output>");
            System.exit(-1);
        }

        Configuration conf1 = new Configuration();
        conf1.setDouble("nn.w1",   INIT_W1);
        conf1.setDouble("nn.w2",   INIT_W2);
        conf1.setDouble("nn.w3",   INIT_W3);
        conf1.setDouble("nn.bias", INIT_BIAS);
        conf1.setDouble("nn.lr",   LR);

        // ── Job 1: Gradiente descendiente (1 época) ─────────────────────────
        Job job1 = Job.getInstance(conf1, "Q5-Job1: Gradiente Descendiente - Perceptron");
        job1.setJarByClass(Query5Driver.class);
        job1.setMapperClass(Query5Mapper1.class);
        job1.setReducerClass(Query5Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        // Un solo reducer para agregar todos los gradientes
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job1 falló.");
            System.exit(1);
        }

        // ── Leer pesos actualizados desde la salida de Job1 ─────────────────
        Configuration conf2 = new Configuration();
        FileSystem fs = FileSystem.get(conf1);
        Path modelPath = new Path(args[1] + "/part-r-00000");

        double w1 = INIT_W1, w2 = INIT_W2, w3 = INIT_W3, bias = INIT_BIAS;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(modelPath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("MODEL")) continue;
                // Formato: MODEL\tw1=X\tw2=Y\tw3=Z\tbias=W\tavg_loss=L\tcount=N
                String[] tokens = line.split("\t");
                for (String tok : tokens) {
                    if (tok.startsWith("w1="))   w1   = Double.parseDouble(tok.substring(3));
                    else if (tok.startsWith("w2="))   w2   = Double.parseDouble(tok.substring(3));
                    else if (tok.startsWith("w3="))   w3   = Double.parseDouble(tok.substring(3));
                    else if (tok.startsWith("bias=")) bias = Double.parseDouble(tok.substring(5));
                }
                break;
            }
        }

        conf2.setDouble("nn.w1",   w1);
        conf2.setDouble("nn.w2",   w2);
        conf2.setDouble("nn.w3",   w3);
        conf2.setDouble("nn.bias", bias);

        // ── Job 2: Evaluación con los pesos aprendidos ───────────────────────
        Job job2 = Job.getInstance(conf2, "Q5-Job2: Evaluacion del Perceptron Entrenado");
        job2.setJarByClass(Query5Driver.class);
        job2.setMapperClass(Query5Mapper2.class);
        job2.setReducerClass(Query5Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[0]));   // re-evalúa sobre el dataset original
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
