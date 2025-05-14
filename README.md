# MATSim Noise Modelling

MATSim Noise Modelling is a Java-based application designed to compute noise exposure and generate noise maps using MATSim simulation data. The application provides a command-line interface (CLI) for configuring and running noise modelling tasks.

## Features

- Compute noise exposure for different time bins.
- Generate noise maps based on simulation data.
- Flexible configuration through a properties file.
- Support for importing OpenStreetMap (OSM) data.
- Export roads and buildings data.
- Clean database and manage simulation results.

## Prerequisites

- Java 11 or higher
- Gradle (build tool)
- Required dependencies are managed via Gradle and Maven repositories.

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd matsim-noisemodelling
   ```

2. Build the project using Gradle:
   ```bash
   ./gradlew build
   ```

3. Run tests to ensure everything is working:
   ```bash
   ./gradlew test
   ```

## Usage

### Running the Application

The main entry point for the application is the `RunCli` class. You can run the application using Gradle:

```bash
./gradlew run --args="<options>"
```

### Command-Line Options

| Option              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `-c, --compute`     | Compute method: `exposure` (default) or `maps`.                            |
| `--conf, --configFile` | Path to the configuration file.                                           |
| `--genconf, --generateConfigFile` | Generate an example configuration file at the specified path. |
| `--tbs, --timeBinSize` | Time bin size in seconds (default: 900).                                 |
| `--tbmin, --timeBinMin` | Time bin minimum in seconds (default: 0).                               |
| `--tbmax, --timeBinMax` | Time bin maximum in seconds (default: 86400).                           |
| `--rec, --receiversMethod` | Receivers method: `closest` (default) or `random`.                   |
| `--diffH, --diffHorizontal` | Enable horizontal diffusion (default: true).                       |
| `--diffV, --diffVertical` | Enable vertical diffusion (default: false).                          |
| `--reflOrder`       | Reflection order (default: 1).                                             |
| `--maxReflDist`     | Maximum reflection distance (default: 50).                                 |
| `--maxSrcDist`      | Maximum source distance (default: 750).                                    |
| `--clean, --cleanDB` | Clean the database.                                                       |
| `--osm, --importOsmPbf` | Import OSM PBF file.                                                   |
| `--roads, --exportRoads` | Export roads data.                                                    |
| `--buildings, --exportBuildings` | Export buildings data.                                        |
| `--run, --runSimulation` | Run the simulation.                                                   |
| `--results, --exportResults` | Export results.                                                   |
| `--all, --doAll`    | Activate all flags (clean database, run everything).                       |
| `-h, --help`        | Display help information.                                                  |

### Example

To generate a configuration file:
```bash
./gradlew run --args="--genconf example-config.properties"
```

To run the application with a configuration file:
```bash
./gradlew run --args="--conf example-config.properties"
```

## Configuration

The application uses a properties file for configuration. Below is an example configuration file:

```properties
DB_NAME=file:///path/to/database
OSM_FILE_PATH=path/to/osm/file.osm.pbf
MATSIM_DIR=path/to/matsim/folder
INPUTS_DIR=path/to/inputs/folder
RESULTS_DIR=path/to/results/folder
SRID=2154
POPULATION_FACTOR=0.1
TIME_BIN_SIZE=900
TIME_BIN_MIN=0
TIME_BIN_MAX=86400
COMPUTE=exposure
RECEIVERS_METHOD=closest
DIFF_HORIZONTAL=True
DIFF_VERTICAL=False
REFL_ORDER=1
MAX_REFL_DIST=50
MAX_SRC_DIST=750
DO_CLEAN_DB=False
DO_IMPORT_OSM=False
DO_EXPORT_ROADS=False
DO_EXPORT_BUILDINGS=False
DO_RUN_NOISEMODELLING=False
DO_EXPORT_RESULTS=False
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Commit your changes and push the branch.
4. Submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

For questions or support, please contact [your-email@example.com].

