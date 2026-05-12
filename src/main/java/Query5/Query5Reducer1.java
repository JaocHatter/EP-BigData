package Query5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Acumula los gradientes de todos los mappers, calcula el promedio
 * y aplica una actualización de gradiente descendiente:
 *   w_nuevo = w_viejo - lr * gradiente_promedio
 *
 * Salida (clave MODEL):
 *   MODEL\tw1=X\tw2=Y\tw3=Z\tbias=W\tavg_loss=L\tcount=N
 */
public class Query5Reducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumDw1 = 0, sumDw2 = 0, sumDw3 = 0, sumDb = 0, sumLoss = 0;
        long count = 0;

        for (Text val : values) {
            String[] p = val.toString().split("\t");
            if (p.length < 5) continue;
            try {
                sumDw1  += Double.parseDouble(p[0]);
                sumDw2  += Double.parseDouble(p[1]);
                sumDw3  += Double.parseDouble(p[2]);
                sumDb   += Double.parseDouble(p[3]);
                sumLoss += Double.parseDouble(p[4]);
                count++;
            } catch (NumberFormatException e) { /* ignorar */ }
        }

        if (count == 0) return;

        double lr = context.getConfiguration().getDouble("nn.lr", Query5Driver.LR);

        double w1   = context.getConfiguration().getDouble("nn.w1",   Query5Driver.INIT_W1);
        double w2   = context.getConfiguration().getDouble("nn.w2",   Query5Driver.INIT_W2);
        double w3   = context.getConfiguration().getDouble("nn.w3",   Query5Driver.INIT_W3);
        double bias = context.getConfiguration().getDouble("nn.bias", Query5Driver.INIT_BIAS);

        // Gradiente promedio y actualización de pesos
        double avgDw1  = sumDw1  / count;
        double avgDw2  = sumDw2  / count;
        double avgDw3  = sumDw3  / count;
        double avgDb   = sumDb   / count;
        double avgLoss = sumLoss / count;

        double newW1   = w1   - lr * avgDw1;
        double newW2   = w2   - lr * avgDw2;
        double newW3   = w3   - lr * avgDw3;
        double newBias = bias - lr * avgDb;

        String modelStr = String.format(
                "w1=%.8f\tw2=%.8f\tw3=%.8f\tbias=%.8f\tavg_loss=%.6f\tcount=%d",
                newW1, newW2, newW3, newBias, avgLoss, count);

        context.write(new Text("MODEL"), new Text(modelStr));
    }
}
