package fr.umrae.matsim_noisemodelling;

import groovy.sql.Sql;
import org.h2.Driver;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Create_Isosurface;
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database;
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Noise_From_Attenuation_Matrix_MatSim;
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Traffic_From_Events;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table;
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM;
import org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source;
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid;

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

class RunComputeMaps {

    static boolean postgis = false;
    static String postgis_db = "postgis_db_nm";
    static String postgis_user = "postgis_user";
    static String postgis_password = "postgis";

    static boolean doCleanDB = false;
    static boolean doImportOSMPbf = false;
    static boolean doExportRoads = false;
    static boolean doExportBuildings = false;
    static boolean doExportResults = false;
    static boolean doTrafficSimulation = false;

    // all flags inside doSimulation
    static boolean doImportMatsimTraffic = true;
    static boolean doIsoNoiseMap = true;

    static int timeBinSize = 900;
    static int timeBinMin = 0;
    static int timeBinMax = 86400;

    // acoustic propagation parameters
    static boolean diffHorizontal = true;
    static boolean diffVertical = false;
    static int reflOrder = 1;
    static int maxReflDist = 50;
    static int maxSrcDist = 750;

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
                params.put("perVehicleLevel", true);
                params.put("populationFactor", populationFactor);

                new Traffic_From_Events().exec(connection, params);
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


                ConnectionWrapper wrapper = new ConnectionWrapper(connection);
                new Noise_level_from_source().exec(wrapper, params);
                sql.execute("DROP TABLE IF EXISTS ATTENUATION_ISO_MAP");
                sql.execute("ALTER TABLE RECEIVERS_LEVEL RENAME TO ATTENUATION_ISO_MAP");

                new Noise_From_Attenuation_Matrix_MatSim().exec(connection, Map.of(
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
                            "IDRECEIVER INTEGER PRIMARY KEY AUTO_INCREMENT, " +
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
                            ") AS SELECT r.IDRECEIVER AS IDRECEIVER, r.THE_GEOM, r.HZ63, r.HZ125, r.HZ250, r.HZ500, " +
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
                    "tableToExport", "TIME_CONTOURING_NOISE_MAP",
                    "exportPath", Paths.get(resultsFolder, "TIME_CONTOURING_NOISE_MAP.geojson")
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