package Query5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Forward-pass + backward-pass por cada registro.
 * Emite gradientes parciales con clave única "GRAD" para que el reducer los acumule.
 *
 * Características (x):
 *   x1 = edad_meses / 60.0          (normalizada 0-1)
 *   x2 = género (M=0.0, F=1.0)
 *   x3 = tipo_diagnóstico (D=1.0, R=0.5, P=0.0)
 *
 * Etiqueta: y = 1 si SEV, 0 en otro caso.
 */
public class Query5Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private double w1, w2, w3, bias;

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

        // Normalizar características
        double x1 = edadMeses / 60.0;
        double x2 = genero.equals("F") ? 1.0 : 0.0;
        double x3;
        switch (tipoDiag) {
            case "D":  x3 = 1.0; break;
            case "R":  x3 = 0.5; break;
            default:   x3 = 0.0; break;
        }

        // Etiqueta binaria
        double y = severidad.equals("SEV") ? 1.0 : 0.0;

        // Forward pass
        double z     = w1 * x1 + w2 * x2 + w3 * x3 + bias;
        double yHat  = sigmoid(z);

        // Pérdida cross-entropy (con clipping para evitar log(0))
        double yHatClipped = Math.max(1e-10, Math.min(1 - 1e-10, yHat));
        double loss = -(y * Math.log(yHatClipped) + (1 - y) * Math.log(1 - yHatClipped));

        // Backward pass (gradiente de cross-entropy con sigmoid: dL/dz = yHat - y)
        double error = yHat - y;
        double dw1 = error * x1;
        double dw2 = error * x2;
        double dw3 = error * x3;
        double db  = error;

        // Emitir gradientes + loss como String delimitado
        String grad = String.format("%.10f\t%.10f\t%.10f\t%.10f\t%.10f",
                dw1, dw2, dw3, db, loss);
        context.write(new Text("GRAD"), new Text(grad));
    }

    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }
}
