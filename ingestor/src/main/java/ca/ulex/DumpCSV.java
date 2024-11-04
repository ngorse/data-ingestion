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
    // Database connection information
    private static final String URL = System.getenv("INGESTOR_DB_URL");
    private static final String USER = System.getenv("INGESTOR_DB_USER");
    private static final String PASSWORD = System.getenv("INGESTOR_DB_PASSWORD");
    private static final String CSV_FILE = System.getenv("INGESTOR_DB_CSV_OUTPUT");

    public static void main(String[] args)
    {
        String query = "SELECT b.id, p.id_brand, p.product_id, cb.id_product, cb.name, " +
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

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE))) {

            writer.write(Utils.CSV_HEADER);
            writer.newLine();

            long startTime = System.currentTimeMillis();
            int linesDumped = 0;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    StringBuilder row = new StringBuilder();
                    row.append(rs.getString("variant_id")).append(",")
                            .append(rs.getString("product_id")).append(",")
                            .append(rs.getString("size_label")).append(",")
                            .append(rs.getString("product_name")).append(",")
                            .append(rs.getString("name")).append(",")
                            .append(rs.getString("color")).append(",")
                            .append(rs.getString("age_group")).append(",")
                            .append(rs.getString("gender")).append(",")
                            .append(rs.getString("size_type")).append(",")
                            .append(rs.getString("product_type"));

                    writer.write(row.toString());
                    writer.newLine(); // Move to the next line
                    linesDumped++;
                    if (linesDumped % 1000 == 0) {
                        System.out.print("\rDumped lines: " + Utils.decimalFormat.format(linesDumped));
                    }
                }

                conn.close();
                System.out.println("\nTotal lines dumped: " + linesDumped);
                System.out.println("Total elapsed time: " + Utils.formatTime(System.currentTimeMillis() - startTime));
            }

            System.out.println("Data dumped successfully to " + CSV_FILE);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
