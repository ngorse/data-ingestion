package ca.ulex;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Ingestor
{
    // Database connection information
    private static final String URL = System.getenv("INGESTOR_DB_URL");
    private static final String USER = System.getenv("INGESTOR_DB_USER");
    private static final String PASSWORD = System.getenv("INGESTOR_DB_PASSWORD");
    private static final String CSV_FILE = System.getenv("INGESTOR_DB_CSV_INPUT");
    // Set to true for debugging, false otherwise
    private static final boolean AUTO_COMMIT = (Boolean.parseBoolean(System.getenv("INGESTOR_DB_AUTOCOMMIT")));

    public static void main(String[] args)
    {
        String csvFilePath = getCSVFilePathOrExit();

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            connection.setAutoCommit(AUTO_COMMIT);
            long startTime = System.currentTimeMillis();
            int linesIngested = ingestDataFromCSV(csvFilePath, connection);
            if (!AUTO_COMMIT) {
                connection.commit();
            }
            System.out.println("\rLines ingested: " + linesIngested);
            System.out.println("Total time    : " + Utils.formatTime(System.currentTimeMillis() - startTime));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getCSVFilePathOrExit()
    {
        if (CSV_FILE.isEmpty()) {
            System.err.println("ERROR: INGESTOR_DB_CSV_FILE env variable is not set");
            System.exit(1);
        }

        File file = new File(CSV_FILE);
        if (!file.exists() || !file.isFile()) {
            System.err.println("ERROR: invalid file: " + CSV_FILE);
            System.exit(1);
        }

        return CSV_FILE;
    }

    private static int ingestDataFromCSV(String csvFilePath, Connection conn) {
        int csvLine = 0;
        try {
            CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));

            String[] headers = csvReader.readNext(); // Read the header row
            String[] line;
            Map<String, Integer> brandIdMap = new HashMap<>();
            Map<String, Integer> productIdMap = new HashMap<>();
            Map<String, Integer> variantIdMap = new HashMap<>();

            while ((line = csvReader.readNext()) != null) {
                csvLine++;
                String variantId = line[0];
                String productId = line[1];
                String sizeLabel = line[2];
                String productName = line[3];
                String brand = line[4];
                String color = line[5];
                String ageGroup = line[6];
                String gender = line[7];
                String sizeType = line[8];
                String productType = line[9];

                int dbProductId = insertProduct(conn, productIdMap, productId, csvLine);
                int dbVariantId = insertVariant(conn, variantIdMap, dbProductId, variantId, sizeType, csvLine);
                insertMetadata(conn, dbVariantId, sizeLabel, productName, brand, color, ageGroup, gender, productType, csvLine);

                if (csvLine % 1000 == 0) {
                    System.out.print("\rIngested lines: " + Utils.decimalFormat.format(csvLine));
                }
            }

            csvReader.close();
        }
        catch (IOException | SQLException | CsvException e) {
            System.out.println("csvLine: " + csvLine);
            e.printStackTrace();
        }

        return csvLine;
    }

    private static int insertProduct(Connection conn, Map<String, Integer> productIdMap, String productId, int csvLine)
    throws SQLException
    {
        if (productIdMap.containsKey(productId)) {
            return productIdMap.get(productId);
        }

        String sql = "INSERT INTO product (csv_line, product_id) VALUES (?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, csvLine);
        stmt.setString(2, productId);
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            int id = rs.getInt(1);
            productIdMap.put(productId, id); // Store in map
            return id;
        }

        // Should throw an exception here
        return -1;
    }

    private static int insertVariant(Connection conn, Map<String, Integer> variantIdMap, int productId, String variantId, String sizeType, int csvLine)
    throws SQLException
    {
        String sql = "INSERT INTO variant (id_product, csv_line, variant_id, size_type) VALUES (?, ?, ?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, productId);
        stmt.setInt(2, csvLine);
        stmt.setString(3, variantId);
        stmt.setString(4, sizeType);
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            return rs.getInt(1);
        }

        // Should throw an exception here
        return -1;
    }

    private static void insertMetadata(Connection conn, int variantId, String sizeLabel, String productName, String brand, String color, String ageGroup, String gender, String productType, int csvLine)
    throws SQLException
    {
        String sql = "INSERT INTO metadata (id_variant, csv_line, size_label, product_name, brand, color, age_group, gender, product_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, variantId);
        stmt.setInt(2, csvLine);
        stmt.setString(3, sizeLabel);
        stmt.setString(4, productName);
        stmt.setString(5, brand);
        stmt.setString(6, color);
        stmt.setString(7, ageGroup);
        stmt.setString(8, gender);
        stmt.setString(9, productType);
        stmt.executeQuery();
    }
}

