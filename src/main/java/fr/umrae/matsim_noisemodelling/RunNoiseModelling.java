package fr.umrae.matsim_noisemodelling;

import groovy.sql.Sql;
import org.apache.commons.cli.*;
import org.h2.Driver;
import org.h2gis.functions.factory.H2GISFunctions;
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Create_Isosurface;
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database;
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.*;
import org.noise_planet.noisemodelling.wps.Geometric_Tools.ZerodB_Source_From_Roads;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM;
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source;
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid;
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

class RunNoiseModelling {

    static boolean postgis = false;
    static String postgis_db = "postgis_db_nm";
    static String postgis_user = "postgis_user";
    static String postgis_password = "postgis";

    static boolean doCleanDB = false;
    static boolean doImportOSMPbf = false;
    static boolean doExportRoads = false;
    static boolean doExportBuildings = false;
    static boolean doImportData = false;
    static boolean doExportResults = false;
    static boolean doTrafficSimulation = false;

    // all flags inside doSimulation
    static boolean doImportMatsimTraffic = true;
    static boolean doCreateReceiversFromMatsim = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doCalculateExposure = true;
    static boolean doIsoNoiseMap = false;

    static int timeBinSize = 900;
    static int timeBinMin = 0;
    static int timeBinMax = 86400;

    static String receiversMethod = "closest";  // random, closest
    static String ignoreAgents = "";

    // acoustic propagation parameters
    static boolean diffHorizontal = true;
    static boolean diffVertical = false;
    static int reflOrder = 1;
    static int maxReflDist = 50;
    static int maxSrcDist = 750;

    public static void main(String[] args) throws SQLException, IOException {
        cli(args);
    }

    public static void cli(String[] args) throws SQLException, IOException {

        Options options = new Options();
        Properties configFile = new Properties();

        options.addOption("conf", "configFile", true, "Config file path");
        options.addOption("genconf", "generateConfigFile", true, "Create an example config file at path");

        options.addOption("tbs", "timeBinSize", true, "Time bin size in seconds (default: 900)");
        options.addOption("tbmin", "timeBinMin", true, "Time bin min in seconds (default: 0)");
        options.addOption("tbmax", "timeBinMax", true, "Time bin max in seconds (default: 86400)");

//        options.addOption("ia", "ignoreAgents", true, "Ignore agents in the simulation");
        options.addOption("rec", "receiversMethod", true, "Receivers method : 'closest' (default) or 'random'");

        options.addOption("diffH", "diffHorizontal", true, "Diffusion horizontal (default: true)");
        options.addOption("diffV", "diffVertical", true, "Diffusion vertical (default: false)");

        options.addOption("reflOrder", "reflOrder", true, "Reflection order (default: 1)");
        options.addOption("maxReflDist", "maxReflDist", true, "Max reflection distance (default: 50)");
        options.addOption("maxSrcDist", "maxSrcDist", true, "Max source distance (default: 750)");

        options.addOption("clean", "cleanDB",false, "Clean the database");
        options.addOption("osm", "importOsmPbf", false, "Import OSM PBF file");
//        options.addOption("import", "importData", false, "Import data");
        options.addOption("roads", "exportRoads", false, "Export roads");
        options.addOption("buildings", "exportBuildings", false, "Export buildings");
        options.addOption("run", "runSimulation", false, "Run simulation");
        options.addOption("results", "exportResults", false, "Export results");
        options.addOption("all", "doAll", false, "Activate all flags (cleans up database and run everything)");

        options.addOption("h", "help", false, "Display help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();
        try {
            cmd = parser.parse(options, args);
        }
        catch (MissingOptionException e) {
            System.out.println("Missing option: " + e.getMessage());
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }
        catch (ParseException e) {
            System.out.println("CLI options error: " + e.getMessage());
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }

        if (cmd.hasOption("help") || cmd.hasOption("h")) {
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }

        if (!cmd.hasOption("configFile") && !cmd.hasOption("generateConfigFile")) {
            System.out.println("Missing config file, to create an example one use -genconf");
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }

        if (cmd.hasOption("generateConfigFile")) {
            String configFilePath = cmd.getOptionValue("generateConfigFile");
            try {
                File file = new File(configFilePath);
                boolean created = file.createNewFile();
                configFile.setProperty("DB_NAME", "file:///path/to/database");
                configFile.setProperty("OSM_FILE_PATH", "path/to/osm/file.osm.pbf");
                configFile.setProperty("MATSIM_DIR", "path/to/matsim/folder");
                configFile.setProperty("INPUTS_DIR", "path/to/inputs/folder");
                configFile.setProperty("RESULTS_DIR", "path/to/results/folder");
                configFile.setProperty("SRID", "2154");
                configFile.setProperty("POPULATION_FACTOR", "0.1");

                configFile.setProperty("timeBinSize", "900");
                configFile.setProperty("timeBinMin", "0");
                configFile.setProperty("timeBinMax", "86400");

                configFile.setProperty("receiversMethod", "closest");
                configFile.setProperty("ignoreAgents", "False");

                configFile.setProperty("diffHorizontal", "True");
                configFile.setProperty("diffVertical", "False");

                configFile.setProperty("reflOrder", "1");
                configFile.setProperty("maxReflDist", "50");
                configFile.setProperty("maxSrcDist", "750");

                configFile.setProperty("DO_CLEAN_DB", "False");
                configFile.setProperty("DO_IMPORT_OSM", "False");
                configFile.setProperty("DO_EXPORT_ROADS", "False");
                configFile.setProperty("DO_EXPORT_BUILDINGS", "False");
                configFile.setProperty("DO_RUN_NOISEMODELLING", "False");
                configFile.setProperty("DO_EXPORT_RESULTS", "False");

                FileOutputStream fileStream = new FileOutputStream(file);
                configFile.store(fileStream, "An example config file for the noise modelling process");
                fileStream.close();

                System.out.println("An example config file created here : " + file.getAbsolutePath());
                System.out.println("Default values are : " );
                configFile.list(System.out);

                return;
            }
            catch (Exception e) {
                System.err.println("Error while creating example config file : " + e.getMessage());
                return;
            }
        }

        try {
            File file = new File(cmd.getOptionValue("configFile"));
            configFile.load(new FileInputStream(file));
        }
        catch (Exception e) {
            System.out.println("Config file not found : " + e.getMessage());
            return;
        }

        String dbName = configFile.get("DB_NAME").toString();
        dbName = dbName.replace("\\", "/");
        try {
            URI uri = URI.create(dbName);
        }
        catch (Exception e) {
            System.err.println("Database file uri is not valid  : " + e.getMessage());
            return;
        }

        String osmFile = configFile.get("OSM_FILE_PATH").toString();
        if (!Files.exists(Paths.get(osmFile))) {
            System.err.println("OSM file does not exist: " + osmFile);
            return;
        }
        String matsimFolder = configFile.get("MATSIM_DIR").toString();
        if (!Files.exists(Paths.get(matsimFolder))) {
            System.err.println("Matsim folder does not exist: " + matsimFolder);
            return;
        }
        String inputsFolder = configFile.get("INPUTS_DIR").toString();
        if (!Files.exists(Paths.get(inputsFolder))) {
            System.out.println("[WARNING] Inputs folder does not exist: " + inputsFolder);
            // we don't have to return as the input folder is not really used
            // return;
        }
        String resultsFolder = configFile.get("RESULTS_DIR").toString();
        int srid = 2154;
        try {
            srid = Integer.parseInt(configFile.get("SRID").toString());
        } catch (Exception e) {
            System.err.println("SRID is not valid: " + e.getMessage());
            return;
        }
        double populationFactor = 1.0;
        try {
            populationFactor = Double.parseDouble(configFile.get("POPULATION_FACTOR").toString());
        } catch (Exception e) {
            System.err.println("Population factor is not valid: " + e.getMessage());
            return;
        }

        try {
            Files.createDirectories(Paths.get(resultsFolder));
        }
        catch (Exception e) {
            System.out.println("Error trying to create the results folder : " + e.getMessage());
            return;
        }

        if (cmd.hasOption("timeBinSize")) {
            timeBinSize = Integer.parseInt(cmd.getOptionValue("timeBinSize"));
        }
        if (cmd.hasOption("timeBinMin")) {
            timeBinMin = Integer.parseInt(cmd.getOptionValue("timeBinMin"));
        }
        if (cmd.hasOption("timeBinMax")) {
            timeBinMax = Integer.parseInt(cmd.getOptionValue("timeBinMax"));
        }
        if (cmd.hasOption("receiversMethod")) {
            if (!Objects.equals(cmd.getOptionValue("receiversMethod"), "closest") && !Objects.equals(cmd.getOptionValue("receiversMethod"), "random")) {
                System.err.println("Receivers method must be 'closest' (default) or 'random'");
                return;
            }
            receiversMethod = cmd.getOptionValue("receiversMethod");
        }
//        if (cmd.hasOption("ignoreAgents")) {
//            ignoreAgents = cmd.getOptionValue("ignoreAgents");
//        }
        if (cmd.hasOption("diffHorizontal")) {
            diffHorizontal = Boolean.parseBoolean(cmd.getOptionValue("diffHorizontal"));
        }
        if (cmd.hasOption("diffVertical")) {
            diffVertical = Boolean.parseBoolean(cmd.getOptionValue("diffVertical"));
        }
        if (cmd.hasOption("reflOrder")) {
            reflOrder = Integer.parseInt(cmd.getOptionValue("reflOrder"));
        }
        if (cmd.hasOption("maxReflDist")) {
            maxReflDist = Integer.parseInt(cmd.getOptionValue("maxReflDist"));
        }
        if (cmd.hasOption("maxSrcDist")) {
            maxSrcDist = Integer.parseInt(cmd.getOptionValue("maxSrcDist"));
        }

        doCleanDB = cmd.hasOption("cleanDB") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_CLEAN_DB"));
        doImportOSMPbf = cmd.hasOption("importOsmPbf") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_IMPORT_OSM"));

        doExportRoads = cmd.hasOption("exportRoads") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_ROADS"));
        doExportBuildings = cmd.hasOption("exportBuildings") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_BUILDINGS"));

        doTrafficSimulation = cmd.hasOption("runSimulation") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_RUN_NOISEMODELLING"));
        doExportResults = cmd.hasOption("exportResults") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_RESULTS"));

        System.out.println("Running NoiseModelling with parameters:");
        System.out.println("dbName: " + dbName);
        System.out.println("osmFile: " + osmFile);
        System.out.println("matsimFolder: " + matsimFolder);
        System.out.println("inputsFolder: " + inputsFolder);
        System.out.println("resultsFolder: " + resultsFolder);
        System.out.println("srid: " + srid);
        System.out.println("populationFactor: " + populationFactor);
        System.out.println("doCleanDB: " + doCleanDB);
        System.out.println("doImportOSMPbf: " + doImportOSMPbf);
        System.out.println("doExportRoads: " + doExportRoads);
        System.out.println("doExportBuildings: " + doExportBuildings);
        System.out.println("doTrafficSimulation: " + doTrafficSimulation);
        System.out.println("doExportResults: " + doExportResults);
        System.out.println("timeBinSize: " + timeBinSize);
        System.out.println("timeBinMin: " + timeBinMin);
        System.out.println("timeBinMax: " + timeBinMax);
        System.out.println("receiversMethod: " + receiversMethod);
        System.out.println("diffHorizontal: " + diffHorizontal);
        System.out.println("diffVertical: " + diffVertical);
        System.out.println("reflOrder: " + reflOrder);
        System.out.println("maxReflDist: " + maxReflDist);
        System.out.println("maxSrcDist: " + maxSrcDist);

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor);
    }

    public static void run(String dbName, String osmFile, String matsimFolder, String inputsFolder, String resultsFolder, int srid, double populationFactor) throws SQLException, IOException {

        Connection connection;
        Sql sql;

        if (postgis) {
            String url = "jdbc:postgresql://localhost/" + postgis_db;
            Properties props = new Properties();
            props.setProperty("user", postgis_user);
            props.setProperty("password", postgis_password);
            connection = DriverManager.getConnection(url, props);
        }
        else {
            File dbFile = new File(URI.create(dbName));
            String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
            Driver.load();
            connection = DriverManager.getConnection(databasePath, "", "");
            H2GISFunctions.load(connection);
        }

        sql = new Sql(connection);

        Files.createDirectories(Paths.get(resultsFolder));

        if (doCleanDB) {
            new Clean_Database().exec(connection, Map.of(
                    "areYouSure", "yes"
            ));
        }
        if (doImportOSMPbf) {
            new Import_OSM().exec(connection, Map.of(
                    "pathFile", osmFile,
                    "targetSRID", srid,
                    "ignoreGround", true,
                    "ignoreBuilding", false,
                    "ignoreRoads", true,
                    "removeTunnels", false
            ));
            sql.execute("DELETE FROM BUILDINGS WHERE ST_IsEmpty(THE_GEOM);");
        }

        if (doImportData) {
            System.out.println("Nothing to import");
        }

        if (doExportBuildings) {
            new Export_Table().exec(connection, Map.of(
                    "tableToExport", "BUILDINGS",
                    "exportPath", Paths.get(resultsFolder, "BUILDINGS.geojson")
            ));
        }

        if (doTrafficSimulation) {

            if (doImportMatsimTraffic) {
                Map<String, Object> params = new HashMap<>();
                params.put("folder", matsimFolder);
                params.put("outTableName", "MATSIM_ROADS");
                params.put("link2GeometryFile", Paths.get(matsimFolder, "detailed_network.csv")); // absolute path
                params.put("timeBinSize", timeBinSize);
                params.put("timeBinMin", timeBinMin);
                params.put("timeBinMax", timeBinMax);
                params.put("skipUnused", true);
                params.put("exportTraffic", true);
                params.put("SRID", srid);
                params.put("ignoreAgents", ignoreAgents);
                params.put("perVehicleLevel", true);
                params.put("populationFactor", populationFactor);

                new Traffic_From_Events().exec(connection, params);
            }
            if (doCreateReceiversFromMatsim) {
                new Building_Grid().exec(connection, Map.of(
                        "delta",  5.0,
                        "tableBuilding", "BUILDINGS",
                        "receiversTableName", "RECEIVERS",
                        "height", 4.0,
                        "fenceTableName", postgis ? null : "BUILDINGS"
                ));
                new Import_Activities().exec(connection, Map.of(
                        "facilitiesPath", Paths.get(matsimFolder, "output_facilities.xml.gz"),
                        "SRID", srid,
                        "outTableName", "ACTIVITIES"
                ));
                if (Objects.equals(receiversMethod, "random")) {
                    new Receivers_From_Activities_Random().exec(connection, Map.of(
                            "activitiesTable", "ACTIVITIES",
                            "buildingsTable", "BUILDINGS",
                            "receiversTable", "RECEIVERS",
                            "outTableName", "ACTIVITIES_RECEIVERS"
                    ));
                } else {
                    new Receivers_From_Activities_Closest().exec(connection, Map.of(
                            "activitiesTable", "ACTIVITIES",
                            "receiversTable", "RECEIVERS",
                            "outTableName", "ACTIVITIES_RECEIVERS"
                    ));
                }
            }

            if (doCalculateNoisePropagation) {
                new ZerodB_Source_From_Roads().exec(connection, Map.of(
                        "roadsTableName", "MATSIM_ROADS",
                        "sourcesTableName", "SOURCES_0DB"
                ));
                Map<String, Object> params = new HashMap<>();
                params.put("tableBuilding", "BUILDINGS");
                params.put("tableReceivers", "ACTIVITIES_RECEIVERS");
                params.put("tableSources", "SOURCES_0DB");
                params.put("confMaxSrcDist", maxSrcDist);
                params.put("confMaxReflDist", maxReflDist);
                params.put("confReflOrder", reflOrder);
                params.put("confSkipLevening", true);
                params.put("confSkipLnight", true);
                params.put("confSkipLden", true);
                params.put("confThreadNumber", 16);
                params.put("confExportSourceId", true);
                params.put("confDiffVertical", diffVertical);
                params.put("confDiffHorizontal", diffHorizontal);

                new Noise_level_from_source().exec(connection, params);

                sql.execute("DROP TABLE IF EXISTS ATTENUATION_TRAFFIC");
                sql.execute("ALTER TABLE RECEIVERS_LEVEL RENAME TO ATTENUATION_TRAFFIC");
            }
            if (doCalculateNoiseMap) {
                Noise_From_Attenuation_Matrix_Local.exec(connection, Map.of(
                        "matsimRoads", "MATSIM_ROADS",
                        "matsimRoadsLw", "MATSIM_ROADS_LW",
                        "attenuationTable", "ATTENUATION_TRAFFIC",
                        "receiversTable", "ACTIVITIES_RECEIVERS",
                        "outTableName", "RESULT_GEOM",
                        "timeBinSize", timeBinSize,
                        "timeBinMin", timeBinMin,
                        "timeBinMax", timeBinMax
                ));
            }

            if (doCalculateExposure) {
                Map<String, Object> params = new HashMap<>();
                params.put("experiencedPlansFile", Paths.get(matsimFolder, "output_experienced_plans.xml.gz"));
                params.put("plansFile", Paths.get(matsimFolder, "output_plans.xml.gz"));
                params.put("personsCsvFile", Paths.get(matsimFolder, "output_persons.csv.gz"));
                params.put("SRID", srid);
                params.put("receiversTable", "ACTIVITIES_RECEIVERS");
                params.put("outTableName", "EXPOSURES");
                params.put("dataTable", "RESULT_GEOM");
                params.put("timeBinSize", timeBinSize);
                params.put("timeBinMin", timeBinMin);
                params.put("timeBinMax", timeBinMax);

                new Agent_Exposure().exec(connection, params);
            }

            if (doIsoNoiseMap) {
                new Delaunay_Grid().exec(connection, Map.of(
                        "tableBuilding", "BUILDINGS",
                        "sourcesTableName", "MATSIM_ROADS",
                        "outputTableName", "ISO_RECEIVERS"
                ));
                Map<String, Object> params = new HashMap<>();
                params.put("tableBuilding", "BUILDINGS");
                params.put("tableReceivers", "ISO_RECEIVERS");
                params.put("tableSources", "SOURCES_0DB");
                params.put("confMaxSrcDist", maxSrcDist);
                params.put("confMaxReflDist", maxReflDist);
                params.put("confReflOrder", reflOrder);
                params.put("confSkipLevening", true);
                params.put("confSkipLnight", true);
                params.put("confSkipLden", true);
                params.put("confThreadNumber", 16);
                params.put("confExportSourceId", true);
                params.put("confDiffVertical", diffVertical);
                params.put("confDiffHorizontal", diffHorizontal);

                new Noise_level_from_source().exec(connection, params);
                sql.execute("DROP TABLE IF EXISTS ATTENUATION_ISO_MAP");
                sql.execute("ALTER TABLE RECEIVERS_LEVEL RENAME TO ATTENUATION_ISO_MAP");

                Noise_From_Attenuation_Matrix_Local.exec(connection, Map.of(
                        "matsimRoads", "MATSIM_ROADS",
                        "matsimRoadsLw", "MATSIM_ROADS_LW",
                        "attenuationTable", "ATTENUATION_ISO_MAP",
                        "receiversTable", "ISO_RECEIVERS",
                        "outTableName", "RESULT_ISO_MAP",
                        "timeBinSize", timeBinSize,
                        "timeBinMin", timeBinMin,
                        "timeBinMax", timeBinMax
                ));
                String dataTable = "RESULT_ISO_MAP";
                String resultTable = "TIME_CONTOURING_NOISE_MAP";

                sql.execute(String.format("DROP TABLE %s IF EXISTS", resultTable));
                String createQuery = "CREATE TABLE " + resultTable + " (" +
                        "PK INTEGER PRIMARY KEY AUTO_INCREMENT, " +
                        "CELL_ID INTEGER, " +
                        "THE_GEOM GEOMETRY, " +
                        "ISOLVL INTEGER, " +
                        "ISOLABEL VARCHAR, " +
                        "TIME INTEGER" +
                        ")";
                sql.execute(createQuery);

                ensureIndex(connection, dataTable, "THE_GEOM", true);

                for (int time = timeBinMin ; time < timeBinMax; time += timeBinSize) {
                    String timeString = String.valueOf(time);
                    String timeDataTable = dataTable + "_" + timeString;

                    sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable));
                    String query = "CREATE TABLE " + timeDataTable + " (" +
                            "PK INTEGER PRIMARY KEY AUTO_INCREMENT, " +
                            "THE_GEOM GEOMETRY, " +
                            "HZ63 DOUBLE PRECISION, " +
                            "HZ125 DOUBLE PRECISION, " +
                            "HZ250 DOUBLE PRECISION, " +
                            "HZ500 DOUBLE PRECISION, " +
                            "HZ1000 DOUBLE PRECISION, " +
                            "HZ2000 DOUBLE PRECISION, " +
                            "HZ4000 DOUBLE PRECISION, " +
                            "HZ8000 DOUBLE PRECISION, " +
                            "TIME INTEGER, " +
                            "LAEQ DOUBLE PRECISION, " +
                            "LEQ DOUBLE PRECISION" +
                            ") AS SELECT r.IDRECEIVER AS PK, r.THE_GEOM, r.HZ63, r.HZ125, r.HZ250, r.HZ500, " +
                            "r.HZ1000, r.HZ2000, r.HZ4000, r.HZ8000, r.TIME, r.LEQA AS LAEQ, r.LEQ " +
                            "FROM " + dataTable + " r WHERE r.TIME = " + time;

                    sql.execute(query);

                    new Create_Isosurface().exec(connection, Map.of(
                            "resultTable", timeDataTable
                    ));

                    sql.execute("INSERT INTO " + resultTable + "(CELL_ID, THE_GEOM, ISOLVL, ISOLABEL, TIME) SELECT cm.CELL_ID, cm.THE_GEOM, cm.ISOLVL, cm.ISOLABEL, " + time + " FROM CONTOURING_NOISE_MAP cm");
                    sql.execute(String.format("DROP TABLE %s IF EXISTS", "CONTOURING_NOISE_MAP"));
                    sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable));
                }
                sql.execute("ALTER TABLE " + resultTable + " ADD TIME_DATE TIME");
                sql.execute("UPDATE " + resultTable + " as e SET TIME_DATE = DATEADD( SECOND , e.TIME, PARSEDATETIME('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss'))");

                new Export_Table().exec(connection, Map.of(
                        "tableToExport", "CONTOURING_NOISE_MAP",
                        "exportPath", Paths.get(resultsFolder, "CONTOURING_NOISE_MAP.geojson")
                ));
            }
        }

        if (doExportRoads) {
            new Export_Table().exec(connection, Map.of(
                    "tableToExport", "MATSIM_ROADS",
                    "exportPath", Paths.get(resultsFolder, "MATSIM_ROADS.geojson")
            ));
        }

        if (doExportResults) {
            new Export_Table().exec(connection, Map.of(
                    "tableToExport", "RESULT_GEOM",
                    "exportPath", Paths.get(resultsFolder, "RESULT_GEOM.shp")
            ));
        }

        connection.close();
    }

    static boolean tableExists(Connection connection, String table) throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getTables(null, null, table, null);
        boolean table_found = false;
        if (rs.next()) {
            table_found = true;
        }
        return table_found;
    }

    static boolean columnExists(Connection connection, String table, String column_name) throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, table, column_name);
        boolean col_found = false;
        if (rs.next()) {
            col_found = true;
        }
        return col_found;
    }

    static boolean indexExists(Connection connection, String table, String column_name) throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getIndexInfo(null, null, table, false, false);
        boolean index_found = false;
        while (rs.next()) {
            String column = rs.getString("COLUMN_NAME");
            String pos = rs.getString("ORDINAL_POSITION");
            if (Objects.equals(column, column_name) && Objects.equals(pos, "1")) {
                index_found = true;
            }
        }
        return index_found;
    }

    static void ensureIndex(Connection connection, String table, String column_name, boolean spatial) throws SQLException {
        if (!indexExists(connection, table, column_name)) {
            Sql sql = new Sql(connection);
            sql.execute("CREATE " + (spatial ? "SPATIAL " : "") + "INDEX ON " + table + " (" + column_name + ")");
        }
    }

}