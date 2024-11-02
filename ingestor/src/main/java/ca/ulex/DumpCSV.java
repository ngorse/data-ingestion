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
        String query = "SELECT v.variant_id, p.product_id, lm.size_label, lm.product_name, " +
                "b.name AS brand, lm.color, v.age_group, v.gender, v.size_type, lm.product_type " +
                "FROM variant v " +
                "JOIN product p ON v.id_product = p.id " +
                "JOIN brand b ON p.id_brand = b.id " +
                "JOIN localized_meta lm ON v.id = lm.id_variant";

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE))) {

            writer.write("variant_id,product_id,size_label,product_name,brand,color,age_group,gender,size_type,product_type");
            writer.newLine();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    StringBuilder row = new StringBuilder();
                    row.append(rs.getString("variant_id")).append(",")
                            .append(rs.getString("product_id")).append(",")
                            .append(rs.getString("size_label")).append(",")
                            .append(rs.getString("product_name")).append(",")
                            .append(rs.getString("brand")).append(",")
                            .append(rs.getString("color")).append(",")
                            .append(rs.getString("age_group")).append(",")
                            .append(rs.getString("gender")).append(",")
                            .append(rs.getString("size_type")).append(",")
                            .append(rs.getString("product_type"));

                    writer.write(row.toString());
                    writer.newLine(); // Move to the next line
                }
            }

            System.out.println("Data dumped successfully to " + CSV_FILE);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
