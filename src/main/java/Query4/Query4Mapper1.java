package Query4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Discretiza la edad en meses a grupo etario y emite la clase:
 *   clave: grupo_etario|género    (ej. "INF|M")
 *   valor: GRADO_SEVERIDAD        (ej. "SEV")
 *
 * Grupos etarios (en meses):
 *   INF  = 0-12   (infantes)
 *   TOD  = 12-24  (toddlers)
 *   YOU  = 24-36  (pequeños)
 *   PRE  = 36-60  (preescolares)
 */
public class Query4Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] c = value.toString().split("\t");
        if (c.length < 21) return;
        try { Integer.parseInt(c[0].trim()); } catch (NumberFormatException e) { return; }

        String genero    = c[2].trim();
        String tipoEdad  = c[4].trim();
        String severidad = c[7].trim();

        if (genero.isEmpty() || tipoEdad.isEmpty() || severidad.isEmpty()) return;
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

        String grupo;
        if      (edadMeses < 12) grupo = "INF";
        else if (edadMeses < 24) grupo = "TOD";
        else if (edadMeses < 36) grupo = "YOU";
        else                     grupo = "PRE";

        context.write(new Text(grupo + "|" + genero), new Text(severidad));
    }
}
