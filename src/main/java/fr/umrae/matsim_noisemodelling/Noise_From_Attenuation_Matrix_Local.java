package fr.umrae.matsim_noisemodelling;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class Noise_From_Attenuation_Matrix_Local {

    static String exec(Connection connection, Map<String, Object> input) throws SQLException {

        connection = new ConnectionWrapper(connection);

        Sql sql = new Sql(connection);

        String resultString;

        Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling");
        logger.info("Start : Noise_From_Attenuation_Matrix");
        logger.info("inputs {}", input);

        String matsimRoads = input.get("matsimRoads").toString();
        String matsimRoadsLw = input.get("matsimRoadsLw").toString();
        String attenuationTable = input.get("attenuationTable").toString();
        String receiversTable = input.get("receiversTable").toString();
        String outTableName = input.get("outTableName").toString();

        int timeBinSize = 3600;
        if (input.get("timeBinSize") != null) {
            timeBinSize = ((int) input.get("timeBinSize"));
        }

        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getIndexInfo(null, null, attenuationTable, false, false);


        sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName));
        String query = "CREATE TABLE " + outTableName + " (" +
            "PK INTEGER PRIMARY KEY AUTO_INCREMENT, " +
            "IDRECEIVER INTEGER, " +
            "THE_GEOM GEOMETRY, " +
            "HZ63 DOUBLE PRECISION, " +
            "HZ125 DOUBLE PRECISION, " +
            "HZ250 DOUBLE PRECISION, " +
            "HZ500 DOUBLE PRECISION, " +
            "HZ1000 DOUBLE PRECISION, " +
            "HZ2000 DOUBLE PRECISION, " +
            "HZ4000 DOUBLE PRECISION, " +
            "HZ8000 DOUBLE PRECISION, " +
            "TIME INT" +
            ")";
        sql.execute(query);
        PreparedStatement insert_stmt = connection.prepareStatement("INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        logger.info("searching indexes on attenuation matrix ... ");
        ensureIndex(connection, attenuationTable, "IDSOURCE", false);
        ensureIndex(connection, attenuationTable, "IDRECEIVER", false);
        logger.info("searching indexes on traffic tables ... ");
        ensureIndex(connection, matsimRoads, "LINK_ID", false);
        ensureIndex(connection, matsimRoadsLw, "LINK_ID", false);
        ensureIndex(connection, matsimRoadsLw, "TIME", false);

        List<String> mrs_freqs = List.of("LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000");

        long count = 0, do_print = 1;
        List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM " + receiversTable);
        long nb_receivers = receivers_res.size();
        long start = System.currentTimeMillis();
        for (GroovyRowResult receiver: receivers_res) {
            long receiver_id = ((Number) receiver.get("PK")).longValue();
            Geometry receiver_geom = (Geometry) receiver.get("THE_GEOM");
            Map<Integer, List<Double>> levels = new HashMap<Integer, List<Double>>();
            List<GroovyRowResult> sources_att_res = sql.rows(String.format("SELECT lg.* FROM %s lg WHERE lg.IDRECEIVER = %d", attenuationTable, receiver_id));
            long nb_sources = sources_att_res.size();
            if (nb_sources == 0) {
                count++;
                continue;
            }
            for (GroovyRowResult sources_att: sources_att_res) {
                long source_id = ((Number) sources_att.get("IDSOURCE")).longValue();
                List<Double> attenuation = new ArrayList<>();
                attenuation.add(((Number) sources_att.get("HZ63")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ125")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ250")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ500")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ1000")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ2000")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ4000")).doubleValue());
                attenuation.add(((Number) sources_att.get("HZ8000")).doubleValue());
                List<GroovyRowResult> roads_stats_res = sql.rows(String.format(
                        "SELECT mrs.* FROM %s mrs INNER JOIN %s mr ON mr.LINK_ID = mrs.LINK_ID WHERE mr.PK = %d",
                        matsimRoadsLw, matsimRoads, source_id));
                for (GroovyRowResult roads_stats: roads_stats_res) {
                    int timeBin = (int) roads_stats.get("TIME");
                    if (!levels.containsKey(timeBin)) {
                        levels.put(timeBin, new ArrayList<>(List.of(-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0)));
                    }
                    for (int i = 0; i < 8; i++) {
                        double new_level = ((double) roads_stats.get(mrs_freqs.get(i))) + attenuation.get(i);
                        new_level = 10 * Math.log10(Math.pow(10, levels.get(timeBin).get(i) / 10) + Math.pow(10, new_level / 10));
                        levels.get(timeBin).set(i, new_level);
                    }
                }
            }

            for (int timeBin = 0; timeBin < 86400; timeBin += timeBinSize) {
                if (!levels.containsKey(timeBin)) {
                    levels.put(timeBin, List.of(-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0));
                }
                List<Double> ts_levels = levels.get(timeBin);
                insert_stmt.setLong(1, receiver_id);
                insert_stmt.setString(2, receiver_geom.toText());
                for (int i = 0; i < 8; i++) {
                    insert_stmt.setDouble(i + 3, ts_levels.get(i));
                }
                insert_stmt.setInt(11, timeBin);
                insert_stmt.execute();
            }
            if (count >= do_print) {
                double elapsed = (double) (System.currentTimeMillis() - start + 1) / 1000;
                logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                        count, nb_receivers, elapsed, count/elapsed));
                do_print *= 2;
            }
            count ++;
        }

        String prefix = "HZ";
        sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))");
        sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))");

        logger.info("End : Noise_From_Attenuation_Matrix");
        resultString = "Process done. Table of receivers " + outTableName + " created !";
        logger.info("Result : " + resultString);
        return resultString;
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
