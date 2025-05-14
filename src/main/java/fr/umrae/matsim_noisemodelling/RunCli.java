package fr.umrae.matsim_noisemodelling;

import groovy.sql.Sql;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class RunCli {

    public static void main(String[] args) throws SQLException, IOException {

        Options options = new Options();
        Properties configFile = new Properties();

        options.addOption("c", "compute", true, "Compute method : 'exposure' (default) or 'maps'");

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

                configFile.setProperty("TIME_BIN_SIZE", "900");
                configFile.setProperty("TIME_BIN_MIN", "0");
                configFile.setProperty("TIME_BIN_MAX", "86400");

                configFile.setProperty("COMPUTE", "exposure");

                configFile.setProperty("RECEIVERS_METHOD", "closest");

                configFile.setProperty("DIFF_HORIZONTAL", "True");
                configFile.setProperty("DIFF_VERTICAL", "False");

                configFile.setProperty("REFL_ORDER", "1");
                configFile.setProperty("MAX_REFL_DIST", "50");
                configFile.setProperty("MAX_SRC_DIST", "750");

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

        String compute = configFile.getOrDefault("COMPUTE", "exposure").toString();
        if (!cmd.hasOption("compute")) {
            System.out.println("Compute method not specified, using default 'exposure'");
        }
        else if (cmd.getOptionValue("compute").isEmpty()) {
            System.err.println("Compute method is empty, using default 'exposure'");
        }
        else {
            if (!Objects.equals(cmd.getOptionValue("compute"), "exposure") && !Objects.equals(cmd.getOptionValue("compute"), "maps")) {
                System.err.println("Compute method must be 'exposure' (default) or 'maps'");
            } else {
                compute = cmd.getOptionValue("compute");
            }
        }


        int timeBinSize =  Integer.parseInt(configFile.getOrDefault("TIME_BIN_SIZE", 900).toString());
        if (cmd.hasOption("timeBinSize")) {
            timeBinSize = Integer.parseInt(cmd.getOptionValue("timeBinSize"));
        }
        int timeBinMin = Integer.parseInt(configFile.getOrDefault("TIME_BIN_MIN", 0).toString());
        if (cmd.hasOption("timeBinMin")) {
            timeBinMin = Integer.parseInt(cmd.getOptionValue("timeBinMin"));
        }
        int timeBinMax = Integer.parseInt(configFile.getOrDefault("TIME_BIN_MAX", 86400).toString());
        if (cmd.hasOption("timeBinMax")) {
            timeBinMax = Integer.parseInt(cmd.getOptionValue("timeBinMax"));
        }
        String receiversMethod = configFile.getOrDefault("RECEIVERS_METHOD", "closest").toString();
        if (cmd.hasOption("receiversMethod")) {
            if (!Objects.equals(cmd.getOptionValue("receiversMethod"), "closest") && !Objects.equals(cmd.getOptionValue("receiversMethod"), "random")) {
                System.err.println("Receivers method must be 'closest' (default) or 'random'");
                return;
            }
            receiversMethod = cmd.getOptionValue("receiversMethod");
        }
        boolean diffHorizontal = Boolean.parseBoolean(configFile.getOrDefault("DIFF_HORIZONTAL", "true").toString());
        if (cmd.hasOption("diffHorizontal")) {
            diffHorizontal = Boolean.parseBoolean(cmd.getOptionValue("diffHorizontal"));
        }
        boolean diffVertical = Boolean.parseBoolean(configFile.getOrDefault("DIFF_VERTICAL", "false").toString());;
        if (cmd.hasOption("diffVertical")) {
            diffVertical = Boolean.parseBoolean(cmd.getOptionValue("diffVertical"));
        }
        int reflOrder =  Integer.parseInt(configFile.getOrDefault("REFL_ORDER", 1).toString());;
        if (cmd.hasOption("reflOrder")) {
            reflOrder = Integer.parseInt(cmd.getOptionValue("reflOrder"));
        }
        int maxReflDist =  Integer.parseInt(configFile.getOrDefault("MAX_REFL_DIST", 50).toString());
        if (cmd.hasOption("maxReflDist")) {
            maxReflDist = Integer.parseInt(cmd.getOptionValue("maxReflDist"));
        }
        int maxSrcDist = Integer.parseInt(configFile.getOrDefault("MAX_SRC_DIST", 750).toString());
        if (cmd.hasOption("maxSrcDist")) {
            maxSrcDist = Integer.parseInt(cmd.getOptionValue("maxSrcDist"));
        }

        boolean doCleanDB = cmd.hasOption("cleanDB") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_CLEAN_DB"));
        boolean doImportOSMPbf = cmd.hasOption("importOsmPbf") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_IMPORT_OSM"));

        boolean doExportRoads = cmd.hasOption("exportRoads") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_ROADS"));
        boolean doExportBuildings = cmd.hasOption("exportBuildings") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_BUILDINGS"));

        boolean doTrafficSimulation = cmd.hasOption("runSimulation") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_RUN_NOISEMODELLING"));
        boolean doExportResults = cmd.hasOption("exportResults") || cmd.hasOption("doAll") || Boolean.parseBoolean((String) configFile.get("DO_EXPORT_RESULTS"));

        System.out.println("Running NoiseModelling with parameters:");
        System.out.println("dbName: " + dbName);
        System.out.println("osmFile: " + osmFile);
        System.out.println("matsimFolder: " + matsimFolder);
        System.out.println("inputsFolder: " + inputsFolder);
        System.out.println("resultsFolder: " + resultsFolder);
        System.out.println("srid: " + srid);
        System.out.println("populationFactor: " + populationFactor);
        System.out.println("compute: " + compute);
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

        if (Objects.equals(compute, "exposure")) {
            RunComputeExposure.doCleanDB = doCleanDB;
            RunComputeExposure.doImportOSMPbf = doImportOSMPbf;
            RunComputeExposure.doExportRoads = doExportRoads;
            RunComputeExposure.doExportBuildings = doExportBuildings;
            RunComputeExposure.doTrafficSimulation = doTrafficSimulation;
            RunComputeExposure.doExportResults = doExportResults;
            RunComputeExposure.timeBinSize = timeBinSize;
            RunComputeExposure.timeBinMin = timeBinMin;
            RunComputeExposure.timeBinMax = timeBinMax;
            RunComputeExposure.receiversMethod = receiversMethod;
            RunComputeExposure.diffHorizontal = diffHorizontal;
            RunComputeExposure.diffVertical = diffVertical;
            RunComputeExposure.reflOrder = reflOrder;
            RunComputeExposure.maxReflDist = maxReflDist;
            RunComputeExposure.maxSrcDist = maxSrcDist;
            RunComputeExposure.run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor);
        }
        else if (Objects.equals(compute, "maps")) {
            RunComputeMaps.doCleanDB = doCleanDB;
            RunComputeMaps.doImportOSMPbf = doImportOSMPbf;
            RunComputeMaps.doExportRoads = doExportRoads;
            RunComputeMaps.doExportBuildings = doExportBuildings;
            RunComputeMaps.doTrafficSimulation = doTrafficSimulation;
            RunComputeMaps.doExportResults = doExportResults;
            RunComputeMaps.timeBinSize = timeBinSize;
            RunComputeMaps.timeBinMin = timeBinMin;
            RunComputeMaps.timeBinMax = timeBinMax;
            RunComputeMaps.diffHorizontal = diffHorizontal;
            RunComputeMaps.diffVertical = diffVertical;
            RunComputeMaps.reflOrder = reflOrder;
            RunComputeMaps.maxReflDist = maxReflDist;
            RunComputeMaps.maxSrcDist = maxSrcDist;
            RunComputeMaps.run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor);
        }

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