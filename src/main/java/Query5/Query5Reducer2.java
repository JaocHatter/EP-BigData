package Query5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Calcula métricas finales del perceptrón entrenado:
 *   - Exactitud   = (TP + TN) / (TP + FP + FN + TN)
 *   - Precisión   = TP / (TP + FP)
 *   - Recall      = TP / (TP + FN)
 *   - F1-Score    = 2 * Precision * Recall / (Precision + Recall)
 *   - Pérdida promedio (cross-entropy)
 *
 * Todas las métricas son decimales de 0.0 a 1.0 (o 0.0 a 100.0 en %).
 */
public class Query5Reducer2 extends Reducer<Text, Text, Text, Text> {

    private long tp = 0, fp = 0, fn = 0, tn = 0;
    private double sumLoss = 0;
    private long   lossCount = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        for (Text val : values) {
            String s = val.toString().trim();
            switch (key.toString()) {
                case "TP":
                case "FP":
                case "FN":
                case "TN":
                    try { count += Long.parseLong(s); } catch (NumberFormatException e) { count++; }
                    break;
                case "LOSS":
                    try {
                        sumLoss += Double.parseDouble(s);
                        lossCount++;
                    } catch (NumberFormatException e) { /* ignorar */ }
                    break;
            }
        }

        switch (key.toString()) {
            case "TP": tp = count; break;
            case "FP": fp = count; break;
            case "FN": fn = count; break;
            case "TN": tn = count; break;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long total = tp + fp + fn + tn;
        if (total == 0) return;

        double exactitud  = (double)(tp + tn) / total;
        double precision  = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0.0;
        double recall     = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0.0;
        double f1         = (precision + recall) > 0
                            ? 2.0 * precision * recall / (precision + recall) : 0.0;
        double avgLoss    = lossCount > 0 ? sumLoss / lossCount : Double.NaN;

        double w1   = context.getConfiguration().getDouble("nn.w1",   Query5Driver.INIT_W1);
        double w2   = context.getConfiguration().getDouble("nn.w2",   Query5Driver.INIT_W2);
        double w3   = context.getConfiguration().getDouble("nn.w3",   Query5Driver.INIT_W3);
        double bias = context.getConfiguration().getDouble("nn.bias", Query5Driver.INIT_BIAS);

        context.write(new Text("PESOS_APRENDIDOS"),
            new Text(String.format("w1(edad)=%.6f  w2(genero)=%.6f  w3(tipoDiag)=%.6f  bias=%.6f",
                w1, w2, w3, bias)));

        context.write(new Text("PERDIDA_PROMEDIO"),
            new Text(String.format("%.6f", avgLoss)));

        context.write(new Text("CONFUSION_MATRIX"),
            new Text(String.format("TP=%d  FP=%d  FN=%d  TN=%d", tp, fp, fn, tn)));

        context.write(new Text("EXACTITUD"),
            new Text(String.format("%.4f  (%.2f%%)", exactitud, exactitud * 100)));

        context.write(new Text("PRECISION"),
            new Text(String.format("%.4f  (%.2f%%)", precision, precision * 100)));

        context.write(new Text("RECALL"),
            new Text(String.format("%.4f  (%.2f%%)", recall, recall * 100)));

        context.write(new Text("F1_SCORE"),
            new Text(String.format("%.4f  (%.2f%%)", f1, f1 * 100)));
    }
}
