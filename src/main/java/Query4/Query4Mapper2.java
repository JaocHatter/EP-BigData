package Query4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lee la distribución de clases de Job1 y extrae:
 *   - Regla de clasificación (hoja → clase predicha)
 *   - Métricas de precisión por hoja (para calcular exactitud global en Reducer2)
 *
 * Emite dos tipos de pares:
 *   ("RULE",    "grupo|género → MAJORITY (conf=XX.XX%, gini=X.XXXX, n=N)")
 *   ("ACCURACY","correcto\ttotal")
 */
public class Query4Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Línea Job1: "grupo|género\tLEV=X,MOD=Y,SEV=Z,TOTAL=N,GINI=G,MAJORITY=C"
        String[] linea = value.toString().split("\t");
        if (linea.length < 2) return;

        String featureCombo = linea[0].trim();
        String distStr      = linea[1].trim();

        Map<String, String> props = new HashMap<>();
        for (String token : distStr.split(",")) {
            String[] kv = token.split("=");
            if (kv.length == 2) props.put(kv[0], kv[1]);
        }

        int lev, mod, sev, total;
        double gini;
        String majority;
        try {
            lev      = Integer.parseInt(props.getOrDefault("LEV", "0"));
            mod      = Integer.parseInt(props.getOrDefault("MOD", "0"));
            sev      = Integer.parseInt(props.getOrDefault("SEV", "0"));
            total    = Integer.parseInt(props.getOrDefault("TOTAL", "0"));
            gini     = Double.parseDouble(props.getOrDefault("GINI", "0"));
            majority = props.getOrDefault("MAJORITY", "MOD");
        } catch (NumberFormatException e) { return; }

        if (total == 0) return;

        int correct;
        switch (majority) {
            case "LEV": correct = lev; break;
            case "MOD": correct = mod; break;
            case "SEV": correct = sev; break;
            default:    correct = 0;
        }
        double confidence = (double) correct / total * 100.0;

        // Regla legible
        String rule = String.format(
                "%s → %s (conf=%.2f%%, gini=%.4f, n=%d)",
                featureCombo, majority, confidence, gini, total);
        context.write(new Text("RULE"), new Text(rule));

        // Contribución a la exactitud global
        context.write(new Text("ACCURACY"), new Text(correct + "\t" + total));
    }
}
