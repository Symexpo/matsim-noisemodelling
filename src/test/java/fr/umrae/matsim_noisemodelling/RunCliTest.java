package fr.umrae.matsim_noisemodelling;


import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RunCliTest {

    static boolean cleanTempDir = false;
    static Path tempDataDir;

    static Path buildingsPath;
    static Path roadsPath;

    // some default values for tests
    int reflOrder = 1;
    int maxReflDist = 10;
    int maxSrcDist = 100;
    int timeBinMin = 3600 * 6;
    int timeBinMax = 3600 * 18;
    int timeBinSize = 3600;

    @BeforeAll
    static void setupTests() throws IOException {
        // URL of the file to download
        String fileUrl = "https://github.com/Symexpo/matsim-noisemodelling/releases/download/v5.0.0/scenario_matsim.zip";

        // Create a temporary directory
        tempDataDir = Files.createTempDirectory("noise_modelling_test_");
        Path zipFilePath = tempDataDir.resolve("scenario_matsim.zip");

        buildingsPath = Path.of(tempDataDir + "/results/BUILDINGS.geojson");
        roadsPath = Path.of(tempDataDir + "/results/MATSIM_ROADS.geojson");

        // Download the file
        try (InputStream in = new URL(fileUrl).openStream()) {
            Files.copy(in, zipFilePath);
        }

        // Unzip the file
        ZipFile zipFile = new ZipFile(zipFilePath.toFile());
        zipFile.stream().forEach(entry -> {
            try {
                Path entryPath = tempDataDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(entryPath);
                } else {
                    Files.createDirectories(entryPath.getParent());
                    try (InputStream in = zipFile.getInputStream(entry)) {
                        Files.copy(in, entryPath);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Clean up
        zipFile.close();

        Properties configFile = new Properties();
        configFile.setProperty("DB_NAME", "file:///" + tempDataDir + "/noisemodelling");
        configFile.setProperty("OSM_FILE_PATH", tempDataDir + "/nantes_mini.osm.pbf");
        configFile.setProperty("MATSIM_DIR", tempDataDir.toString());
        configFile.setProperty("INPUTS_DIR", tempDataDir + "/inputs");
        configFile.setProperty("RESULTS_DIR", tempDataDir + "/results");
        configFile.setProperty("SRID", "2154");
        configFile.setProperty("POPULATION_FACTOR", "0.001");

        configFile.store(Files.newOutputStream(tempDataDir.resolve("noisemodelling.properties")), null);

    }

    @AfterAll
    static void cleanup() throws IOException {
        if (cleanTempDir) {
            // Delete the temporary directory and its contents
            Files.walk(tempDataDir)
                    .sorted((a, b) -> b.compareTo(a)) // Sort in reverse order to delete files before directories
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    @Test
    @Order(1)
    void testHelp() throws SQLException, IOException {
        String[] args = {"--help"};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        assertTrue(true);
    }

    @Test
    @Order(2)
    void testCliGenerateConfig() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/example.properties");
        String[] args = {"-genconf", paramPath.toString()};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        assertTrue(paramPath.toFile().exists());
    }

    @Test
    @Order(3)
    void testCliConfig() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/noisemodelling.properties");
        String[] args = {"-conf", paramPath.toString()};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        assertTrue(paramPath.toFile().exists());
    }

    @Test
    @Order(4)
    void testCliOsm() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/noisemodelling.properties");
        String[] args = {"-conf", paramPath.toString(), "-osm"};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        assertTrue(true);
    }


    @Test
    @Order(5)
    void testCliExportBuidlings() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/noisemodelling.properties");
        String[] args = {"-conf", paramPath.toString(), "--exportBuildings"};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        Path buildingsPath = Path.of(tempDataDir + "/results/BUILDINGS.geojson");
        assertTrue(buildingsPath.toFile().exists());
    }

    @Test
    @Order(10)
    void testCliRunExposure() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/noisemodelling.properties");
        String[] args = {
                "-conf", paramPath.toString(),
                "-osm",
                "--runSimulation",
                "-results",
                "--reflOrder", String.valueOf(reflOrder),
                "--maxReflDist", String.valueOf(maxReflDist),
                "--maxSrcDist", String.valueOf(maxSrcDist),
                "--timeBinMin", String.valueOf(timeBinMin),
                "--timeBinMax", String.valueOf(timeBinMax),
                "--timeBinSize", String.valueOf(timeBinSize)
        };
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        Path resultPath = Path.of(tempDataDir + "/results/RESULT_GEOM.shp");
        assertTrue(resultPath.toFile().exists());
    }

    @Test
    @Order(10)
    void testCliRunMaps() throws IOException, SQLException {
        Path paramPath = Path.of(tempDataDir + "/noisemodelling.properties");
        String[] args = {
                "-conf", paramPath.toString(),
                "-c", "maps",
                "-osm",
                "--runSimulation",
                "-results",
                "--reflOrder", String.valueOf(0),
                "--maxReflDist", String.valueOf(10),
                "--maxSrcDist", String.valueOf(50),
                "--timeBinMin", String.valueOf(3600 * 8),
                "--timeBinMax", String.valueOf(3600 * 10),
                "--timeBinSize", String.valueOf(3600)
        };
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        Path resultPath = Path.of(tempDataDir + "/results/TIME_CONTOURING_NOISE_MAP.geojson");
        assertTrue(resultPath.toFile().exists());
    }

    @Test
    @Order(99)
    void testCleanDB() throws SQLException, IOException {
        String[] args = {"-conf", tempDataDir + "/noisemodelling.properties", "-clean"};
        RunCli.main(args);
        // Check that the main method runs without throwing an exception
        assertTrue(true);
    }

}
