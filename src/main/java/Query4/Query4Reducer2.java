package Query4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Consolida las reglas del árbol de decisión y calcula la exactitud global de entrenamiento.
 *
 * Salida:
 *   ACCURACY → "Exactitud_entrenamiento=XX.XX% (correcto/total)"
 *   RULE     → listado de reglas con confianza y Gini por hoja
 */
public class Query4Reducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String keyStr = key.toString();

        if (keyStr.equals("ACCURACY")) {
            long sumCorrect = 0, sumTotal = 0;
            for (Text val : values) {
                String[] p = val.toString().split("\t");
                if (p.length < 2) continue;
                try {
                    sumCorrect += Long.parseLong(p[0]);
                    sumTotal   += Long.parseLong(p[1]);
                } catch (NumberFormatException e) { /* ignorar */ }
            }
            if (sumTotal == 0) return;
            double exactitud = (double) sumCorrect / sumTotal * 100.0;
            String res = String.format(
                    "Exactitud_entrenamiento=%.2f%% (%d / %d correctos)",
                    exactitud, sumCorrect, sumTotal);
            context.write(key, new Text(res));

        } else if (keyStr.equals("RULE")) {
            List<String> rules = new ArrayList<>();
            for (Text val : values) rules.add(val.toString());
            // Encabezado seguido de cada regla numerada
            for (int i = 0; i < rules.size(); i++) {
                context.write(new Text("RULE_" + (i + 1)), new Text(rules.get(i)));
            }
        }
    }
}
