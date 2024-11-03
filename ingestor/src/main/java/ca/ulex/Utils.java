package ca.ulex;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

public class Utils
{
    static DecimalFormat decimalFormat = new DecimalFormat("#,###");

    public static String formatTime(long durationMillis)
    {
        long hours = TimeUnit.MILLISECONDS.toHours(durationMillis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationMillis) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis) % 60;
        long millis = TimeUnit.MILLISECONDS.toMillis(durationMillis) % 1000;

        return String.format("%dh %02dm %02d.%03ds", hours, minutes, seconds, millis);
    }
}
