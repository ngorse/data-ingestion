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
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.commons.text.similarity.JaccardSimilarity;

public class Ingestor
{
    final static String[] LOCALES = {"zh-CN", "en", "fr", "de", "it", "ja", "ko", "pt", "ru", "es"};
    final static LanguageDetector detector = initializeLanguageDetector();
    final static String WARNING_EMPTY_FIELD = "Line dropped: No content for field ";
    final static String WARNING_INVALID_BRAND = "Line dropped: Brand name too different from others";
    final static double SIMILARITY_THRESHOLD=0.8;

    public static void main(String[] args) {
        String dbUrl = System.getenv("INGESTOR_DB_URL");
        String dbUser = System.getenv("INGESTOR_DB_USER");
        String dbPassword = System.getenv("INGESTOR_DB_PASSWORD");
        boolean autoCommit = Boolean.parseBoolean(System.getenv("INGESTOR_DB_AUTOCOMMIT"));
        String csvFilePath = System.getenv("INGESTOR_DB_CSV_INPUT");

        Utils.exitOnInvalidCSVFilePath(csvFilePath);

        try (Connection dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            dbConnection.setAutoCommit(autoCommit);

            ingestCSV(csvFilePath, dbConnection);

            if (!autoCommit) {
                dbConnection.commit();
            }
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

    private static void ingestCSV(String csvFilePath, Connection dbConnection) {
        Map<String, Integer> productMap = new HashMap<>();
        Map<String, Integer> variantMap = new HashMap<>();
        Map<String, Integer> brandMap = new HashMap<>();
        long startTime = System.currentTimeMillis();
        long postProcessStartTime = System.currentTimeMillis();
        long stopTime = System.currentTimeMillis();

        int linesIngested = 0;
        int linesDropped = 0;
        int csvLine = 0;
        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
            String[] header = csvReader.readNext();
            if (header == null || header.length < 10 || !Arrays.equals(header, Utils.CSV_HEADER)) {
                throw new IllegalArgumentException("Invalid CSV header");
            }

            String[] line;
            while ((line = csvReader.readNext()) != null) {
                csvLine++;

                int emptyField = hasEmptyField(line);
                if (emptyField > 0) {
                    insertWarning(dbConnection, csvLine, "WARNING_EMPTY_FIELD", WARNING_EMPTY_FIELD + emptyField);
                    linesDropped++;
                    continue;
                }

                linesIngested++;

                int idProduct = insertProductAndBrand(dbConnection, productMap, brandMap, csvLine,
                        line[1],                       // product_id
                        Utils.normalizeText(line[4])   // brand
                );
                int idVariant = insertVariant(dbConnection, variantMap, idProduct, csvLine,
                        line[0],                       // variant_id
                        Utils.normalizeText(line[6]),  // age_group
                        Utils.normalizeText(line[7]),  // gender
                        line[8].toLowerCase()          // size_type
                );
                insertLocalizedMeta(dbConnection, idVariant, csvLine,
                        line[2].toUpperCase(),         // size_label
                        Utils.normalizeText(line[3]),  // product_name
                        Utils.normalizeText(line[5]),  // color
                        line[9]                        // product_type
                );

                if (linesIngested % 1000 == 0) {
                    System.out.print("\rIngested lines: " + Utils.DECIMAL_FORMAT.format(linesIngested) + "+");
                }
            }

            postProcessStartTime = System.currentTimeMillis();
            postProcessBrandNamesForProducts(dbConnection);
            stopTime = System.currentTimeMillis();

        } catch (IOException | SQLException | CsvException e) {
            System.out.println("Error on csvLine: " + csvLine);
            e.printStackTrace();
        }

        System.out.println("Total lines ingested: " + linesIngested);
        System.out.println("Total lines dropped: " + linesDropped);
        System.out.println("Total elapsed time : " + Utils.formatTime(stopTime - startTime));
        System.out.println("    Ingestion      : " + Utils.formatTime(postProcessStartTime - startTime));
        System.out.println("    Post-process   : " + Utils.formatTime(stopTime - postProcessStartTime));
    }

    public static void postProcessBrandNamesForProducts(Connection dbConnection) throws SQLException {
        System.out.println("\nPost processing...");
        String sql = "SELECT p.product_id, cb.name FROM product p JOIN csv_brand cb ON p.id = cb.id_product";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            Map<String, Map<String, Integer>> productBrandFrequency = populateBrandFrequencyMap(rs);
            printMostFrequentBrands(productBrandFrequency);
        }
    }

    private static Map<String, Map<String, Integer>> populateBrandFrequencyMap(ResultSet rs) throws SQLException {
        Map<String, Map<String, Integer>> productBrandFrequency = new HashMap<>();

        while (rs.next()) {
            String productId = rs.getString("product_id");
            String brandName = rs.getString("name");

            productBrandFrequency
                    .computeIfAbsent(productId, k -> new HashMap<>())
                    .merge(brandName, 1, Integer::sum);
        }
        return productBrandFrequency;
    }

    private static void printMostFrequentBrands(Map<String, Map<String, Integer>> productBrandFrequency) {
        for (Map.Entry<String, Map<String, Integer>> entry : productBrandFrequency.entrySet()) {
            String productId = entry.getKey();
            Map<String, Integer> brandMap = entry.getValue();

            if (brandMap.size() > 1) {
                String mostFrequentBrand = findMostFrequentBrand(brandMap);
                System.out.println("Product ID: " + productId + " - Most Frequent Brand Name: " + mostFrequentBrand +
                        " - { " + brandMap.keySet() + " }");
                checkBrandNameDistance(brandMap.keySet());
            }
        }
    }

    private static String findMostFrequentBrand(Map<String, Integer> brandMap) {
        return brandMap.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private static void checkBrandNameDistance(Set<String> brandNames) {
        Map<String, Integer> similarityMap = calculateSimilarityMap(brandNames);
        printOutliers(brandNames, similarityMap);
    }

    private static Map<String, Integer> calculateSimilarityMap(Set<String> brandNames) {
        Map<String, Integer> similarityMap = new HashMap<>();

        for (String brand1 : brandNames) {
            int similarityCount = 0;
            for (String brand2 : brandNames) {
                if (!brand1.equalsIgnoreCase(brand2) && computeSimilarity(brand1, brand2) >= SIMILARITY_THRESHOLD) {
                    similarityCount++;
                }
            }
            similarityMap.put(brand1, similarityCount);
        }
        return similarityMap;
    }

    private static void printOutliers(Set<String> brandNames, Map<String, Integer> similarityMap) {
        for (Map.Entry<String, Integer> entry : similarityMap.entrySet()) {
            if (entry.getValue() < 1) {
                System.out.println("- Found outlier: " + entry.getKey() + " - {" + brandNames + "}");
                if (similarityMap.size() < 3) {
                    break;
                }
            }
        }
    }

    private static double computeSimilarity(String s1, String s2) {
        String ss1 = s1.toLowerCase().replaceAll("[^a-z0-9]", "").trim();
        String ss2 = s2.toLowerCase().replaceAll("[^a-z0-9]", "").trim();

        if (isSubset(s1, s2) || isSubset(s2, s1)) {
            return 1;
        }
        JaroWinklerSimilarity jaroWinkler = new JaroWinklerSimilarity();
        double similarity1 = jaroWinkler.apply(ss1, ss2);
        JaccardSimilarity jaccard = new JaccardSimilarity();
        double similarity2 = jaccard.apply(ss2, ss1);

        return Math.max(similarity1, similarity2);
    }

    public static boolean isSubset(String s1, String s2) {
        Set<Character> set1 = toSet(s1.toLowerCase().replaceAll("[^a-z0-9]", "").trim());
        Set<Character> set2 = toSet(s2.toLowerCase().replaceAll("[^a-z0-9]", "").trim());
        return set2.containsAll(set1);
    }

    private static Set<Character> toSet(String str) {
        Set<Character> set = new HashSet<>();
        for (char c : str.toCharArray()) {
            set.add(c);
        }
        return set;
    }

    private static void insertWarning(Connection dbConnection, int csvLine, String warning, String description) throws SQLException, IOException {
        String sql = "INSERT INTO warnings (csv_line, warning, description) VALUES (?, ?, ?)";
        PreparedStatement stmt = dbConnection.prepareStatement(sql);
        stmt.setInt(1, csvLine);
        stmt.setString(2, warning);
        stmt.setString(3, description);
        stmt.executeUpdate();
    }

    private static int hasEmptyField(String[] line) {
        for (int i = 0; i < line.length; i++) {
            if (line[i] == null || line[i].isEmpty()) {
                return (i+1);
            }
        }
        return -1;
    }

    private static int insertProductAndBrand(Connection dbConnection, Map<String, Integer> productMap,
                                             Map<String, Integer> brandMap,
                                             int csvLine, String productId, String brand) throws SQLException {
        // Check if the product already exists in the map
        if (productMap.containsKey(productId)) {
            int idProduct = productMap.get(productId);
            insertCsvBrand(dbConnection, idProduct, csvLine, brand);
            return idProduct;
        }

        // Insert brand and product if it doesn't exist
        int idBrand = insertBrand(dbConnection, brandMap, brand);
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

    private static int insertBrand(Connection dbConnection, Map<String, Integer> brandMap, String brand) throws SQLException {
        if (brandMap.containsKey(brand)) {
            return brandMap.get(brand);
        }

        String sql = "INSERT INTO brand (name) VALUES (?) RETURNING id";
        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, brand);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int brandId = rs.getInt(1);
                    brandMap.put(brand, brandId);
                    return brandId;
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

