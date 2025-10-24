# Running Calculations and Riskassessments

This section describes how to run calculations and risk assessments using REIA. It covers the necessary configuration files, input data, and execution commands to perform these tasks effectively.

## Models
To run calculations and risk assessments, you need to have the following models prepared:
- **Exposure Model**
- **Vulnerability Model**
- **Fragility Model**
Please refer to the openquake documentation for detailed instructions on creating these models. Currently the REIA software only supports the model formats used in the example below.

### Load the models into the engine
Use the cli commands to load the models into the REIA engine, all paths relative to the working directory.

```bash
reia exposure add docs/example/exposure_model_converted.xml exposure_model_1
reia vulnerability add docs/example/structural_vulnerability_model.xml structural_model
reia vulnerability add docs/example/nonstructural_vulnerability_model.xml nonstructural_model
reia vulnerability add docs/example/contents_vulnerability_model.xml contents_model
reia vulnerability add docs/example/downtime_vulnerability_model.xml downtime_model
reia vulnerability add docs/example/occupants_vulnerability_model.xml occupants_model

reia fragility add docs/example/structural_fragility_model.xml structural_fragility

reia taxonomymap add docs/example/taxonomy_mapping.csv taxonomy_map
```

## Run Calculation
To run a calculation, you need to create a job configuration file (INI format) that specifies the calculation as you would normally do with OpenQuake.

### Simple Calculation
A quick and simple way is to create a single job configuration file, specifying the vulnerability and fragility models. REIA will automatically run a separate loss and damage calculation:

```ini
[general]
description = scenario risk example calculation
aggregate_by = state
master_seed = 42

[exposure]
exposure_file = 1

[hazard]
random_seed = 42
truncation_level = 2.0
asset_hazard_distance = 200.0
number_of_ground_motion_fields = 100
intensity_measure_types = PGA,SA(0.1),SA(0.3),SA(0.4)
gmfs_csv = docs/example/gmfs.csv
sites_csv = docs/example/sites.csv

[vulnerability]
structural_vulnerability_file = 1
nonstructural_vulnerability_file = 2
contents_vulnerability_file = 3
business_interruption_vulnerability_file = 4
occupants_vulnerability_file = 5
taxonomy_mapping_csv = 1

[fragility]
structural_fragility_file = 1
taxonomy_mapping_csv = 1
```

In the places where you usually would specify the file paths to the model files, you instead provide the IDs of the models you previously loaded into the REIA engine.

the gmfs_csv and sites_csv parameters should point to the respective CSV files containing ground motion fields and site information. For more information on those files, please refer to the OpenQuake documentation. For further options on providing the hazard input data, see below.

The "number_of_ground_motion_fields" parameter defines how many ground motion fields were simulated in the gmfs data input file (number of event realizations).

Once you have created the job configuration file, you can run the calculation using the following command:

```bash
reia risk-assessment run docs/example/job.ini
```

This command will execute the risk assessment based on the parameters defined in the job configuration file. The results will be stored in the REIA engine and can be accessed or exported as needed.

### Specifying Calculations Separately
Alternatively you can specify the loss and damage calculation separately, or even run ensemble models. You can specify this accordingly in the `run` command.

Separate ini files for loss and damage:

```bash
reia risk-assessment run --config risk.ini --config damage.ini --weight 1 --weight 1
```

The weight parameters define the relative weight of each calculation in the final results respective to the risk type, ie. damage or loss.

### Ensemble Models
Those weights can then be used to run ensemble models, ie. combining multiple differently configured calculations for loss and damage respectively:

```bash
reia risk-assessment run --config risk.ini --weight 0.3 --config risk2.ini --weight 0.7 --config damage.ini --weight 0.5 --config damage2.ini --weight 0.5
```

## Hazard Input Options
You can also provide the ground motion fields via different options, as described below. The relevant hazard parameters stay the same:
```ini
[hazard]
random_seed  = 568
asset_hazard_distance = 2
truncation_level = 2.0
intensity_measure_types = SA(0.3), SA(0.6)
number_of_ground_motion_fields = 500
minimum_intensity = {"SA(0.3)": 0.10, "SA(0.6)": 0.05}
```

Where the `intensity_measure_types` and `number_of_ground_motion_fields` are required to specify the used intensity measures and the number of ground motion fields to be generated, or already computed, from the provided hazard source.

An additional custon section `[hazard_source]` is added to specify the source type if an alternative to the CSV format is used as described in the following section.

### OpenQuake CSV format
The simplest way to provide hazard input data is via the `gmfs_csv` and `sites_csv` parameters in the job configuration file as shown above.

```ini
[hazard]
...
gmfs_csv = docs/example/gmfs.csv
sites_csv = docs/example/sites.csv
```

The gmfs are expected to be sampled and in the final format as expected by OpenQuake. No `[hazard_source]` section is required in this case.

### USGS ShakeMap XML format
You can also provide shakemaps in the USGS ShakeMap XML format, in the same manner as the CSV format above:

```ini
[hazard_source]
source_type = shakemap
grid_xml = grid.xml
uncertainty_xml = uncertainty.xml
```

### OpenQuake Hazard Datastore

You can use an OpenQuake Datastore, containing precomputed hazard data, to provide the ground motion fields for the calculation. For this, you can simply specify the datastore path:

```ini
[hazard_source]
source_type = dstore
dstore_file = nepal.hdf5
```

### Shakemap in CSV format
Alternatively, you can provide shakemaps in CSV format. Providing `mean` and `std` columns for the intensity measure types specified, the shakemaps will be sampled internally to generate the ground motion fields.

For this, you can add an additional custom section in the job configuration file:

```ini
[hazard_source]
source_type = custom_csv
gmf_cols = psa03_%g, psa06_%g
uncertainty_cols = lnpsa03_uncertainty, lnpsa06_uncertainty
files = psa03.csv, psa06.csv
```

With the csv files in this the respective format. One or multiple files can be specified (with one or multiple IMTs per file), if multiple files are provided, they will be combined by matching the lat/lon coordinates, coordinates not present in all files will be fully ignored (inner join).  
Standard units used by USGS shakemaps are expected. Ie. %g for acceleration, cm/s for velocity, etc. The order of the IMT's should match the order specified in the `intensity_measure_types` parameter.

```csv
lat,lon,psa03_%g,lnpsa03_uncertainty
47.8,6.0,1.155,0.6082
47.8,6.0083,1.161,0.6082
47.8,6.0167,1.168,0.6082
47.8,6.025,1.1749999999999998,0.6082
```

## Day and Night
You can either specify the time of day in the job `ini` file, as you normally would with OpenQuake:

```ini
[risk_calculation]
time_event = day
```

Or you can specify the actual time via the CLI command when running the calculation:

```bash
reia risk-assessment run docs/example/job.ini --time 2025-10-2025T13:00:00
```

The standard time periods can be configured in the settings (reia/config/settings.py).
- day: 09:00 - 17:00
- night: 20:00 - 07:00
- transit: 07:00 - 09:00 and 17:00 - 19:00