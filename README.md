# matsim-noisemodelling

Create a Noise Map from a MATSim Simulation.

## CLI Usage

The `RunNoiseModelling` class can be executed from the command line. Below is the general syntax:

```bash
gradlew run args="..."
```

### Options

| Option                  | Description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `--timeBinSize`         | Size of the time bins in seconds.                                           |
| `--timeBinMin`          | Minimum time value for processing.                                         |
| `--timeBinMax`          | Maximum time value for processing.                                         |
| `--maxSrcDist`          | Maximum source distance for noise calculation.                             |
| `--maxReflDist`         | Maximum reflection distance for noise calculation.                         |
| `--reflOrder`           | Reflection order for noise calculation.                                    |
| `--diffVertical`        | Enable/disable vertical diffraction.                                        |
| `--diffHorizontal`      | Enable/disable horizontal diffraction.                                      |
| `--resultsFolder`       | Path to the folder where results will be saved.                            |
| `--doIsoNoiseMap`       | Enable/disable the generation of an ISO noise map.                         |
| `--doExportRoads`       | Enable/disable exporting MATSim roads to a GeoJSON file.                   |
| `--doExportResults`     | Enable/disable exporting the final results to a shapefile.                 |

### Example

```bash
gradlew run args="
  --timeBinSize 3600 \
  --timeBinMin 0 \
  --timeBinMax 86400 \
  --maxSrcDist 500 \
  --maxReflDist 200 \
  --reflOrder 2 \
  --diffVertical true \
  --diffHorizontal true \
  --resultsFolder ./results \
  --doIsoNoiseMap true \
  --doExportRoads true \
  --doExportResults true \
 "
```

## Running Tests

To execute tests, run:

```bash
./gradlew test
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
```

This `README.md` provides CLI documentation, explains the steps in `RunNoiseModelling`, and includes additional project details.