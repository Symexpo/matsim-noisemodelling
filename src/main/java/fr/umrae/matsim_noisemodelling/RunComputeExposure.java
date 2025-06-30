package fr.umrae.matsim_noisemodelling;

import groovy.sql.Sql;
import org.h2.Driver;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database;
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.*;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM;
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source;
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class RunComputeExposure {

    public static boolean postgis = false;
    public static String postgis_db = "postgis_db_nm";
    public static String postgis_user = "postgis_user";
    public static String postgis_password = "postgis";

    public static boolean doCleanDB = false;
    public static boolean doImportOSMPbf = false;
    public static boolean doExportRoads = false;
    public static boolean doExportBuildings = false;
    public static boolean doImportData = false;
    public static boolean doExportResults = false;
    public static boolean doTrafficSimulation = false;

    // all flags inside doSimulation
    public static boolean doImportMatsimTraffic = true;
    public static boolean doCreateReceiversFromMatsim = true;
    public static boolean doCalculateNoisePropagation = true;
    public static boolean doCalculateNoiseMap = true;
    public static boolean doCalculateExposure = true;
    public static boolean doIsoNoiseMap = false;

    public static int timeBinSize = 900;
    public static int timeBinMin = 0;
    public static int timeBinMax = 86400;

    public static String receiversMethod = "closest";  // random, closest
    public static String ignoreAgents = "";

    // acoustic propagation parameters
    public static boolean diffHorizontal = true;
    public static boolean diffVertical = false;
    public static int reflOrder = 1;
    public static int maxReflDist = 50;
    public static int maxSrcDist = 750;

    public static void main(String[] args) throws SQLException, IOException {
        // the purpose of this main method is to be run from an IDE after editing the parameters
        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_20p/nantes_commune/noisemodelling/noisemodelling";
        String osmFile = "D:\\SYMEXPO\\osm_maps\\nantes_aire_urbaine.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\noisemodelling\\inputs\\";
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\noisemodelling\\results\\";
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\simulation_output\\";
        int srid = 2154;
        double populationFactor = 0.20;

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
            connection = new ConnectionWrapper(connection);
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

                sql.execute("UPDATE MATSIM_ROADS SET THE_GEOM = ST_SetSrid(THE_GEOM, " + srid + ")");
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
                Map<String, Object> params = new HashMap<>();
                params.put("tableBuilding", "BUILDINGS");
                params.put("tableReceivers", "ACTIVITIES_RECEIVERS");
                params.put("tableSources", "MATSIM_ROADS");
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
                new Noise_From_Attenuation_Matrix_MatSim().exec(connection, Map.of(
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