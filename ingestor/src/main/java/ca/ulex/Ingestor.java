package ca.ulex;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

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
    public static void main(String[] args)
    {
        final String URL = System.getenv("INGESTOR_DB_URL");
        final String USER = System.getenv("INGESTOR_DB_USER");
        final String PASSWORD = System.getenv("INGESTOR_DB_PASSWORD");
        final boolean AUTO_COMMIT = (Boolean.parseBoolean(System.getenv("INGESTOR_DB_AUTOCOMMIT")));
        final String CSV_FILE = System.getenv("INGESTOR_DB_CSV_INPUT");
        Utils.exitOnInvalidCSVFilePath(CSV_FILE);

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            conn.setAutoCommit(AUTO_COMMIT);
            long startTime = System.currentTimeMillis();

            int linesIngested = ingestCSV(CSV_FILE, conn);

            if (!AUTO_COMMIT) {
                conn.commit();
            }
            conn.close();

            System.out.println("\nTotal lines ingested: " + linesIngested);
            System.out.println("Total elapsed time  : " + Utils.formatTime(System.currentTimeMillis() - startTime));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int ingestCSV(String csvFilePath, Connection conn)
    {
        int csvLine = 0;
        try {
            CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));

            /** maps
                productMap <product_id, id_product>
             */
            Map<String, Integer> productMap = new HashMap<>();
            Map<String, Integer> variantMap = new HashMap<>();

            /** get line
             */
            // RFE -- Should validate the header instead of skipping it
            csvReader.readNext();

            String[] line;
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

                int idProduct = insertProductAndBrand(conn, productMap, productId, csvLine, brand);
                int idVariant = insertVariant(conn, variantMap, idProduct, variantId, ageGroup, gender, sizeType, csvLine);
                insertLocalizedMeta(conn, idVariant, csvLine, sizeLabel, productName, color, productType);

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

    private static int insertProductAndBrand(Connection conn, Map<String, Integer> productMap,
                                             String productId, int csvLine, String brand)
            throws SQLException
    {
        /**
         IF product_id exists in product_map:
             map - get entry in productMap: product_id -> id_product
             sql add entry in csv_brand (id_product, csv_line, name)
         ELSE:
            sql - add entry in brand (name), get id_brand
            sql - add entry in product (id_brand, product_id)
            map - add entry in productMap (product_id, id_product)
            sql - add entry in csv_brand (id_product, csv_line, name)
         */

        if (productMap.containsKey(productId)) {
            int idProduct = productMap.get(productId);
            insertCsvBrand(conn, idProduct, csvLine, brand);
            return idProduct;
        }
        else {
            int idBrand = insertBrand(conn, brand);
            String sql = "INSERT INTO product (id_brand, product_id) VALUES (?, ?) RETURNING id";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, idBrand);
            stmt.setString(2, productId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int idProduct = rs.getInt(1);
                productMap.put(productId, idProduct);
                insertCsvBrand(conn, idProduct, csvLine, brand);
                return idProduct;
            }
        }

        // Should throw an exception here
        return -1;
    }

    private static int insertBrand(Connection conn, String brand)
    throws SQLException
    {
        String sql = "INSERT INTO brand (name) VALUES (?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, brand);
        ResultSet rs = stmt.executeQuery();

        if (rs.next()) {
            int id = rs.getInt(1);
            return id;
        }
        else {
            // Should throw an exception here
        }

        return -1;
    }

    private static void insertCsvBrand(Connection conn, int idProduct, int csvLine, String brand)
            throws SQLException
    {
        String sql = "INSERT INTO csv_brand (id_product, csv_line, name) VALUES (?, ?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, idProduct);
        stmt.setInt(2, csvLine);
        stmt.setString(3, brand);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            // Should throw an exception here
        }
    }


    private static int insertVariant(Connection conn, Map<String, Integer> variantMap, int idProduct,
                                     String variantId, String ageGroup, String gender, String sizeType, int csvLine)
    throws SQLException
    {
        /**
         IF variant_id exists in variant_map:
            map - get entry in variantMap: variant_id -> id_variant
            sql add entry in csv_age_group (id_variant, csv_line, age_group)
            sql add entry in csv_gender (id_variant, csv_line, gender)
            sql alter gender_male |= gender == male
            sql alter gender_female |= gender == female
            sql alter gender_unisex |= gender == unisex
         ELSE:
            sql - add entry in variant (id_product, variant_id, age_group, gender_male, gender_female, gender_unisex, size_type)
            map - add entry in variantMap (variant_id, id_variant)
            sql add entry in csv_age_group (id_variant, csv_line, age_group)
            sql add entry in csv_gender (id_variant, csv_line, gender)
         */

        if (variantMap.containsKey(variantId)) {
            int idVariant = variantMap.get(variantId);
            insertCsvAgeGroup(conn, idVariant, csvLine, ageGroup);
            insertCsvGender(conn, idVariant, csvLine, gender);
            updateGender(conn, gender, idVariant);
            return idVariant;
        }
        else {
            String sql = "INSERT INTO variant (id_product, variant_id, age_group, gender_male, gender_female, gender_unisex, size_type) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, idProduct);
            stmt.setString(2, variantId);
            stmt.setString(3, ageGroup);
            stmt.setBoolean(4, gender.equalsIgnoreCase("male"));
            stmt.setBoolean(5, gender.equalsIgnoreCase("female"));
            stmt.setBoolean(6, gender.equalsIgnoreCase("unisex"));
            stmt.setString(7, sizeType);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int idVariant = rs.getInt(1);
                variantMap.put(variantId, idVariant);
                insertCsvAgeGroup(conn, idVariant, csvLine, ageGroup);
                insertCsvGender(conn, idVariant, csvLine, gender);
                return idVariant;
            }
        }

        // Should throw an exception here
        return -1;
    }

    private static void updateGender(Connection conn, String gender, int idVariant)
    throws SQLException
    {
        if (gender.equalsIgnoreCase("male") ||
                gender.equalsIgnoreCase("female") ||
                gender.equalsIgnoreCase("unisex")
        ) {
            String sql = "UPDATE variant SET gender_" + gender.toLowerCase() + " = true WHERE id = ?";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, idVariant);
            stmt.executeUpdate();
        }
        else {
            // RFE -- record warning
        }
    }

    private static void insertCsvAgeGroup(Connection conn, int idVariant, int csvLine, String ageGroup)
    throws SQLException
    {
        String sql = "INSERT INTO csv_age_group (id_variant, csv_line, age_group) VALUES (?, ?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, idVariant);
        stmt.setInt(2, csvLine);
        stmt.setString(3, ageGroup);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            // RFE -- throw an exception here
        }
    }

    private static void insertCsvGender(Connection conn, int idVariant, int csvLine, String gender)
            throws SQLException
    {
        String sql = "INSERT INTO csv_gender (id_variant, csv_line, gender) VALUES (?, ?, ?) RETURNING id";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setInt(1, idVariant);
        stmt.setInt(2, csvLine);
        stmt.setString(3, gender);
        ResultSet rs = stmt.executeQuery();

        if (!rs.next()) {
            // RFE -- throw an exception here
        }
    }

    private static void insertLocalizedMeta(Connection conn, int idVariant, int csvLine, String sizeLabel,
                                            String productName, String color, String productType)
    throws SQLException
    {
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

    private static int OLD_ingestDataFromCSV(String csvFilePath, Connection conn) {
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

                int dbProductId = insertProductAndBrand(conn, productIdMap, productId, csvLine, brand);
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


}

