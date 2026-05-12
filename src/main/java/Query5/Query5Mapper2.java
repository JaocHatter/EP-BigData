package Query5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Aplica los pesos entrenados (cargados desde la Configuración) para clasificar cada registro.
 * Emite métricas para calcular exactitud, precisión y recall:
 *   ("TP", "1")  → verdadero positivo  (predijo SEV, era SEV)
 *   ("FP", "1")  → falso positivo      (predijo SEV, no era SEV)
 *   ("FN", "1")  → falso negativo      (no predijo SEV, era SEV)
 *   ("TN", "1")  → verdadero negativo
 *   ("LOSS", pérdida_cross_entropy)
 */
public class Query5Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private double w1, w2, w3, bias;
    private static final Text ONE = new Text("1");

    @Override
    protected void setup(Context context) {
        w1   = context.getConfiguration().getDouble("nn.w1",   Query5Driver.INIT_W1);
        w2   = context.getConfiguration().getDouble("nn.w2",   Query5Driver.INIT_W2);
        w3   = context.getConfiguration().getDouble("nn.w3",   Query5Driver.INIT_W3);
        bias = context.getConfiguration().getDouble("nn.bias", Query5Driver.INIT_BIAS);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] c = value.toString().split("\t");
        if (c.length < 21) return;
        try { Integer.parseInt(c[0].trim()); } catch (NumberFormatException e) { return; }

        String genero    = c[2].trim();
        String tipoEdad  = c[4].trim();
        String severidad = c[7].trim();
        String tipoDiag  = c[9].trim();

        if (severidad.isEmpty() || genero.isEmpty()) return;
        if (!severidad.equals("LEV") && !severidad.equals("MOD") && !severidad.equals("SEV")) return;

        int edadRaw;
        try { edadRaw = Integer.parseInt(c[3].trim()); } catch (NumberFormatException e) { return; }

        double edadMeses;
        switch (tipoEdad) {
            case "A": edadMeses = edadRaw * 12.0; break;
            case "M": edadMeses = edadRaw;         break;
            case "D": edadMeses = edadRaw / 30.0; break;
            default:  return;
        }
        if (edadMeses < 0 || edadMeses > 60) return;

        double x1 = edadMeses / 60.0;
        double x2 = genero.equals("F") ? 1.0 : 0.0;
        double x3;
        switch (tipoDiag) {
            case "D":  x3 = 1.0; break;
            case "R":  x3 = 0.5; break;
            default:   x3 = 0.0; break;
        }

        double y    = severidad.equals("SEV") ? 1.0 : 0.0;
        double z    = w1 * x1 + w2 * x2 + w3 * x3 + bias;
        double yHat = sigmoid(z);

        // Pérdida cross-entropy
        double yHatClipped = Math.max(1e-10, Math.min(1 - 1e-10, yHat));
        double loss = -(y * Math.log(yHatClipped) + (1 - y) * Math.log(1 - yHatClipped));
        context.write(new Text("LOSS"), new Text(String.format("%.10f", loss)));

        // Matriz de confusión
        boolean predSev  = yHat >= 0.5;
        boolean actualSev = severidad.equals("SEV");

        if      ( predSev &&  actualSev) context.write(new Text("TP"), ONE);
        else if ( predSev && !actualSev) context.write(new Text("FP"), ONE);
        else if (!predSev &&  actualSev) context.write(new Text("FN"), ONE);
        else                             context.write(new Text("TN"), ONE);
    }

    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }
}
