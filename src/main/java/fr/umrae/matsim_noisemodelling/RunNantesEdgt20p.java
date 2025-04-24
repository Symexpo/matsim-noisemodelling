package fr.umrae.matsim_noisemodelling;

import java.io.IOException;
import java.sql.SQLException;

public class RunNantesEdgt20p {
    public static void main(String[] args) throws SQLException, IOException {
        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_20p/nantes_commune/noisemodelling/noisemodelling";
        String osmFile = "D:\\SYMEXPO\\osm_maps\\nantes_aire_urbaine.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\noisemodelling\\inputs\\";
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\noisemodelling\\results\\";
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_commune\\simulation_output\\";
        int srid = 2154;
        double populationFactor = 0.20;

        RunNoiseModelling.doCleanDB = false;
        RunNoiseModelling.doImportOSMPbf = false;

        RunNoiseModelling.doExportRoads = false;
        RunNoiseModelling.doExportBuildings = false;

        RunNoiseModelling.doTrafficSimulation = false;
        RunNoiseModelling.doExportResults = false;

        // all flags inside doSimulation
        RunNoiseModelling.doImportMatsimTraffic = false;
        RunNoiseModelling.doCreateReceiversFromMatsim = false;
        RunNoiseModelling.doCalculateNoisePropagation = false;
        RunNoiseModelling.doCalculateNoiseMap = false;
        RunNoiseModelling.doCalculateExposure = false;
        RunNoiseModelling.doIsoNoiseMap = false;

        RunNoiseModelling.run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor);
    }
}
