package ca.ulex;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DumpCSV
{
    public static void main(String[] args) {
        String URL = System.getenv("INGESTOR_DB_URL");
        String USER = System.getenv("INGESTOR_DB_USER");
        String PASSWORD = System.getenv("INGESTOR_DB_PASSWORD");
        String CSV_FILE = System.getenv("INGESTOR_DB_CSV_OUTPUT");

        String query = buildQuery();

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE))) {

            writer.write(Utils.CSV_HEADER);
            writer.newLine();

            long startTime = System.currentTimeMillis();
            int linesDumped = 0;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    writeRow(writer, rs);
                    linesDumped++;
                    if (linesDumped % 1000 == 0) {
                        System.out.print("\rDumped lines: " + Utils.DECIMAL_FORMAT.format(linesDumped));
                    }
                }

                System.out.println("\nTotal lines dumped: " + linesDumped);
                System.out.println("Total elapsed time: " + Utils.formatTime(System.currentTimeMillis() - startTime));
            }

            System.out.println("Data dumped successfully to " + CSV_FILE);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static String buildQuery() {
        return "SELECT b.id, p.id_brand, p.product_id, cb.id_product, cb.name, " +
                "v.variant_id, cag.age_group, cg.gender, v.size_type, " +
                "m.size_label, m.product_name, m.color, m.product_type " +
                "FROM product p " +
                "JOIN brand b ON p.id_brand = b.id " +
                "JOIN csv_brand cb ON cb.id_product = p.id " +
                "JOIN variant v ON v.id_product = p.id " +
                "JOIN csv_age_group cag ON cag.id_variant = v.id " +
                "JOIN csv_gender cg ON cg.id_variant = v.id " +
                "JOIN localized_meta m ON m.id_variant = v.id " +
                "WHERE cag.csv_line = cg.csv_line AND cb.csv_line = cg.csv_line AND m.csv_line = cg.csv_line";
    }

    private static void writeRow(BufferedWriter writer, ResultSet rs) throws SQLException, IOException {
        String row = String.join(",",
                rs.getString("variant_id"),
                rs.getString("product_id"),
                rs.getString("size_label"),
                rs.getString("product_name"),
                rs.getString("name"),
                rs.getString("color"),
                rs.getString("age_group"),
                rs.getString("gender"),
                rs.getString("size_type"),
                rs.getString("product_type"));

        writer.write(row);
        writer.newLine();
    }
}
