package ca.ulex;

import java.io.File;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

public class Utils
{
    public static final String CSV_HEADER="variant_id,product_id,size_label,product_name,brand,color,age_group,gender,size_type,product_type";

    static DecimalFormat decimalFormat = new DecimalFormat("#,###");
    public static String formatTime(long durationMillis)
    {
        long hours = TimeUnit.MILLISECONDS.toHours(durationMillis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationMillis) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis) % 60;
        long millis = TimeUnit.MILLISECONDS.toMillis(durationMillis) % 1000;

        return String.format("%dh %02dm %02d.%03ds", hours, minutes, seconds, millis);
    }

    public static void exitOnInvalidCSVFilePath(String csvFile)
    {
        if (csvFile.isEmpty()) {
            System.err.println("ERROR: INGESTOR_DB_CSV_FILE env variable is not set");
            System.exit(1);
        }

        File file = new File(csvFile);
        if (!file.exists() || !file.isFile()) {
            System.err.println("ERROR: invalid file: " + csvFile);
            System.exit(1);
        }
    }
}
