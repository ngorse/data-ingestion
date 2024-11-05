package ca.ulex;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Ingestor
{
    final static String[] LOCALES = {"zh-CN", "en", "fr", "de", "it", "ja", "ko", "pt", "ru", "es"};
    final static LanguageDetector detector = initializeLanguageDetector();

    public static void main(String[] args) {
        String dbUrl = System.getenv("INGESTOR_DB_URL");
        String dbUser = System.getenv("INGESTOR_DB_USER");
        String dbPassword = System.getenv("INGESTOR_DB_PASSWORD");
        boolean autoCommit = Boolean.parseBoolean(System.getenv("INGESTOR_DB_AUTOCOMMIT"));
        String csvFilePath = System.getenv("INGESTOR_DB_CSV_INPUT");

        Utils.exitOnInvalidCSVFilePath(csvFilePath);

        try (Connection dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            dbConnection.setAutoCommit(autoCommit);
            long startTime = System.currentTimeMillis();

            int linesIngested = ingestCSV(csvFilePath, dbConnection);

            if (!autoCommit) {
                dbConnection.commit();
            }

            System.out.println("\nTotal lines ingested: " + linesIngested);
            System.out.println("Total elapsed time  : " + Utils.formatTime(System.currentTimeMillis() - startTime));
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static LanguageDetector initializeLanguageDetector()
    {
        Set<String> languages = new HashSet<>(Arrays.asList(LOCALES));
        try {
            return new OptimaizeLangDetector().loadModels(languages);
        } catch (IOException e) {
            return new OptimaizeLangDetector().loadModels();
        }
    }

    private static int ingestCSV(String csvFilePath, Connection dbConnection) {
        Map<String, Integer> productMap = new HashMap<>();
        Map<String, Integer> variantMap = new HashMap<>();
        int csvLine = 0;

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
            // Read and validate the header
            String[] header = csvReader.readNext();
            if (header == null || header.length < 10) {
                throw new IllegalArgumentException("Invalid CSV header");
            }

            String[] line;
            while ((line = csvReader.readNext()) != null) {
                csvLine++;
                int idProduct = insertProductAndBrand(dbConnection, productMap, csvLine,
                        line[1],  // product_id
                        line[4]   // brand
                );
                int idVariant = insertVariant(dbConnection, variantMap, idProduct, csvLine,
                        line[0],  // variant_id
                        line[6],  // age_group
                        line[7],  // gender
                        line[8]   // size_type
                );
                insertLocalizedMeta(dbConnection, idVariant, csvLine,
                        line[2],  // size_label
                        line[3],  // product_name
                        line[5],  // color
                        line[9]   // product_type
                );

                if (csvLine % 1000 == 0) {
                    System.out.print("\rIngested lines: " + Utils.decimalFormat.format(csvLine));
                }
            }
        } catch (IOException | SQLException | CsvException e) {
            System.out.println("Error on csvLine: " + csvLine);
            e.printStackTrace();
        }

        return csvLine;
    }

    private static int insertProductAndBrand(Connection dbConnection, Map<String, Integer> productMap,
                                             int csvLine, String productId, String brand) throws SQLException {
        // Check if the product already exists in the map
        if (productMap.containsKey(productId)) {
            int idProduct = productMap.get(productId);
            insertCsvBrand(dbConnection, idProduct, csvLine, brand);
            return idProduct;
        }

        // Insert brand and product if it doesn't exist
        int idBrand = insertBrand(dbConnection, brand);
        String sql = "INSERT INTO product (id_brand, product_id) VALUES (?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idBrand);
            stmt.setString(2, productId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int idProduct = rs.getInt(1);
                    productMap.put(productId, idProduct);
                    insertCsvBrand(dbConnection, idProduct, csvLine, brand);
                    return idProduct;
                }
            }
        }

        throw new SQLException("Failed to insert product and brand for CSV line: " + csvLine);
    }

    private static int insertBrand(Connection dbConnection, String brand) throws SQLException {
        String sql = "INSERT INTO brand (name) VALUES (?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, brand);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }

        throw new SQLException("Failed to insert brand: " + brand);
    }

    private static void insertCsvBrand(Connection dbConnection, int idProduct, int csvLine, String brand) throws SQLException {
        String sql = "INSERT INTO csv_brand (id_product, csv_line, name) VALUES (?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idProduct);
            stmt.setInt(2, csvLine);
            stmt.setString(3, brand);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to insert CSV brand for product ID: " + idProduct + " at CSV line: " + csvLine);
                }
            }
        }
    }

    private static int insertVariant(Connection dbConnection, Map<String, Integer> variantMap, int idProduct,
                                     int csvLine, String variantId, String ageGroup, String gender, String sizeType) throws SQLException {
        if (variantMap.containsKey(variantId)) {
            int idVariant = variantMap.get(variantId);
            insertCsvAgeGroup(dbConnection, idVariant, csvLine, ageGroup);
            insertCsvGender(dbConnection, idVariant, csvLine, gender);
            updateGender(dbConnection, gender, idVariant);
            return idVariant;
        }

        String sql = "INSERT INTO variant (id_product, variant_id, age_group, gender_male, gender_female, gender_unisex, size_type) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idProduct);
            stmt.setString(2, variantId);
            stmt.setString(3, ageGroup);
            stmt.setBoolean(4, "male".equalsIgnoreCase(gender));
            stmt.setBoolean(5, "female".equalsIgnoreCase(gender));
            stmt.setBoolean(6, "unisex".equalsIgnoreCase(gender));
            stmt.setString(7, sizeType);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int idVariant = rs.getInt(1);
                    variantMap.put(variantId, idVariant);
                    insertCsvAgeGroup(dbConnection, idVariant, csvLine, ageGroup);
                    insertCsvGender(dbConnection, idVariant, csvLine, gender);
                    return idVariant;
                }
            }
        }

        throw new SQLException("Failed to insert variant for ID: " + variantId + " at CSV line: " + csvLine);
    }

    private static void updateGender(Connection dbConnection, String gender, int idVariant) throws SQLException {
        String normalizedGender = gender.toLowerCase();

        if (isValidGender(normalizedGender)) {
            String sql = "UPDATE variant SET gender_" + normalizedGender + " = true WHERE id = ?";

            try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
                stmt.setInt(1, idVariant);
                stmt.executeUpdate();
            }
        } else {
            // Record a warning for invalid gender
            recordInvalidGenderWarning(gender, idVariant);
        }
    }

    private static boolean isValidGender(String gender) {
        return "male".equals(gender) || "female".equals(gender) || "unisex".equals(gender);
    }

    private static void recordInvalidGenderWarning(String gender, int idVariant) {
        // Implement warning logging here
        System.err.println("Warning: Invalid gender '" + gender + "' for variant ID: " + idVariant);
    }

    private static void insertCsvAgeGroup(Connection dbConnection, int idVariant, int csvLine, String ageGroup) throws SQLException {
        String sql = "INSERT INTO csv_age_group (id_variant, csv_line, age_group) VALUES (?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idVariant);
            stmt.setInt(2, csvLine);
            stmt.setString(3, ageGroup);

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Insertion failed, no ID returned for variant ID: " + idVariant);
                }
            }
        }
    }

    private static void insertCsvGender(Connection dbConnection, int idVariant, int csvLine, String gender) throws SQLException {
        String sql = "INSERT INTO csv_gender (id_variant, csv_line, gender) VALUES (?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idVariant);
            stmt.setInt(2, csvLine);
            stmt.setString(3, gender);

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Insertion failed, no ID returned for variant ID: " + idVariant);
                }
            }
        }
    }

    private static void insertLocalizedMeta(Connection dbConnection, int idVariant, int csvLine,
                                            String sizeLabel, String productName, String color, String productType) throws SQLException {
        String sql = "INSERT INTO localized_meta(id_variant, csv_line, locale, size_label, product_name, color, product_type) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, idVariant);
            stmt.setInt(2, csvLine);

            // Detect language
            String locale = detector.detect(productName + " " + color + " " + productType.replace(">", " ")).getLanguage();
            stmt.setString(3, locale);

            // Set other parameters
            stmt.setString(4, sizeLabel);
            stmt.setString(5, productName);
            stmt.setString(6, color);
            stmt.setString(7, productType);

            // Execute the insert
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Insertion failed, no ID returned for variant ID: " + idVariant);
                }
            }
        }
    }

}

