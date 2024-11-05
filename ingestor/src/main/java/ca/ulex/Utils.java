package ca.ulex;

import java.io.File;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

public class Utils
{
    public static final String CSV_HEADER="variant_id,product_id,size_label,product_name,brand,color,age_group,gender,size_type,product_type";

    public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#,###");

    public static String formatTime(long durationMillis) {
        long hours = TimeUnit.MILLISECONDS.toHours(durationMillis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationMillis) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis) % 60;
        long millis = durationMillis % 1000; // Simplified to avoid unnecessary conversion

        return String.format("%dh %02dm %02d.%03ds", hours, minutes, seconds, millis);
    }

    public static void exitOnInvalidCSVFilePath(String csvFile) {
        if (isInvalidPath(csvFile)) {
            System.err.println("ERROR: Invalid CSV file path: " + csvFile);
            System.exit(1);
        }
    }

    private static boolean isInvalidPath(String csvFile) {
        return csvFile == null || csvFile.isEmpty() || !isValidFile(csvFile);
    }

    private static boolean isValidFile(String csvFile) {
        File file = new File(csvFile);
        return file.exists() && file.isFile();
    }

    public static String normalizeText(String input) {
        if (input == null || input.isEmpty()) {
            return input; // Return as is if input is null or empty
        }
        String lowerCaseString = input.toLowerCase();
        return Character.toUpperCase(lowerCaseString.charAt(0)) + lowerCaseString.substring(1);
    }

}
