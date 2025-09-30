Full list of configuration parameters
=====================================

Engine Version: %s

Some parameters have a default that it is used when the parameter is
not specified in the job.ini file. Some other parameters have no default,
which means that not specifying them will raise an error when running
a calculation for which they are required.

override_vs30:
  Optional Vs30 parameter to override the site model Vs30
  Example: *override_vs30 = 800*
  Default: None

aggregate_by:
  Used to compute aggregate losses and aggregate loss curves in risk
  calculations. Takes in input one or more exposure tags.
  Example: *aggregate_by = region, taxonomy*.
  Default: empty list

aggregate_loss_curves_types:
  Used for event-based risk and damage calculations, to estimate the aggregated
  loss Exceedance Probability (EP) only or to also calculate (if possible) the
  Occurrence Exceedance Probability (OEP) and/or the Aggregate Exceedance
  Probability (AEP).
  Example: *aggregate_loss_curves_types = aep, oep*.
  Default: ep

reaggregate_by:
  Used to perform additional aggregations in risk calculations. Takes in
  input a proper subset of the tags in the aggregate_by option.
  Example: *reaggregate_by = region*.
  Default: empty list

amplification_method:
  Used in classical PSHA calculations to amplify the hazard curves with
  the convolution or kernel method.
  Example: *amplification_method = kernel*.
  Default: "convolution"

asce_version:
  ASCE version used in AELO mode.
  Example: *asce_version = asce7-22*.
  Default: "asce7-16"

area_source_discretization:
  Discretization parameters (in km) for area sources.
  Example: *area_source_discretization = 10*.
  Default: 10

ash_wet_amplification_factor:
  Used in volcanic risk calculations.
  Example: *ash_wet_amplification_factor=1.0*.
  Default: 1.0

asset_correlation:
  Used in risk calculations to take into account asset correlation. Accepts
  only the values 1 (full correlation) and 0 (no correlation).
  Example: *asset_correlation=1*.
  Default: 0

asset_hazard_distance:
  In km, used in risk calculations to print a warning when there are assets
  too distant from the hazard sites. In multi_risk calculations can be a
  dictionary: asset_hazard_distance = {'ASH': 50, 'LAVA': 10, ...}
  Example: *asset_hazard_distance = 5*.
  Default: 15

asset_life_expectancy:
  Used in the classical_bcr calculator.
  Example: *asset_life_expectancy = 50*.
  Default: no default

assets_per_site_limit:
  INTERNAL

gmf_max_gb:
  If the size (in GB) of the GMFs is below this value, then compute avg_gmf
  Example: *gmf_max_gb = 1.*
  Default: 0.1

avg_losses:
  Used in risk calculations to compute average losses.
  Example: *avg_losses=false*.
  Default: True

base_path:
  INTERNAL

cache_distances:
  Useful in UCERF calculations.
  Example: *cache_distances = true*.
  Default: False

calculation_mode:
  One of classical, disaggregation, event_based, scenario, scenario_risk,
  scenario_damage, event_based_risk, classical_risk, classical_bcr.
  Example: *calculation_mode=classical*
  Default: no default

collapse_gsim_logic_tree:
  INTERNAL

collapse_level:
  INTERNAL

collect_rlzs:
  Collect all realizations into a single effective realization. If not given
  it is true for sampling and false for full enumeration.
  Example: *collect_rlzs=true*.
  Default: None

correlation_cutoff:
  Used in conditioned GMF calculation to avoid small negative eigenvalues
  wreaking havoc with the numerics
  Example: *correlation_cutoff = 1E-11*
  Default: 1E-12

compare_with_classical:
  Used in event based calculation to perform also a classical calculation,
  so that the hazard curves can be compared.
  Example: *compare_with_classical = true*.
  Default: False

complex_fault_mesh_spacing:
  In km, used to discretize complex faults.
  Example: *complex_fault_mesh_spacing = 15*.
  Default: 5

concurrent_tasks:
  A hint to the engine for the number of tasks to generate. Do not set
  it unless you know what you are doing.
  Example: *concurrent_tasks = 100*.
  Default: twice the number of cores

conditional_loss_poes:
  Used in classical_risk calculations to compute loss curves.
  Example: *conditional_loss_poes = 0.01 0.02*.
  Default: empty list

cholesky_limit:
  When generating the GMFs from a ShakeMap the engine needs to perform a
  Cholesky decomposition of a matrix of size (M x N)^2, being M the number
  of intensity measure types and N the number of sites. The decomposition
  can become ultra-slow, run out of memory, or produce bogus negative
  eigenvalues, therefore there is a limit on the maximum size of M x N.
  Example: *cholesky_limit = 1000*.
  Default: 10,000

continuous_fragility_discretization:
  Used when discretizing continuuos fragility functions.
  Example: *continuous_fragility_discretization = 10*.
  Default: 20

coordinate_bin_width:
  Used in disaggregation calculations.
  Example: *coordinate_bin_width = 1.0*.
  Default: 100 degrees, meaning don't disaggregate by lon, lat

countries:
  Used to restrict the exposure to a single country in Aristotle mode.
  Example: *countries = ITA*.
  Default: ()

cross_correlation:
  When used in Conditional Spectrum calculation is the name of a cross
  correlation class (i.e. "BakerJayaram2008").
  When used in ShakeMap calculations the valid choices are "yes", "no" "full",
  same as for *spatial_correlation*.
  Example: *cross_correlation = no*.
  Default: "yes"

description:
  A string describing the calculation.
  Example: *description = Test calculation*.
  Default: "no description"

disagg_bin_edges:
  A dictionary where the keys can be: mag, dist, lon, lat, eps and the
  values are lists of floats indicating the edges of the bins used to
  perform the disaggregation.
  Example: *disagg_bin_edges = {'mag': [5.0, 5.5, 6.0, 6.5]}*.
  Default: empty dictionary

disagg_by_src:
  Flag used to enable disaggregation by source when possible.
  Example: *disagg_by_src = true*.
  Default: False

disagg_outputs:
  Used in disaggregation calculations to restrict the number of exported
  outputs.
  Example: *disagg_outputs = Mag_Dist*
  Default: list of all possible outputs

discard_assets:
  Flag used in risk calculations to discard assets from the exposure.
  Example: *discard_assets = true*.
  Default: False

discard_trts:
  Used to discard tectonic region types that do not contribute to the hazard.
  Example: *discard_trts = Volcanic*.
  Default: empty list

discrete_damage_distribution:
  Make sure the damage distribution contain only integers (require the "number"
  field in the exposure to be integer).
  Example: *discrete_damage_distribution = true*
  Default: False

distance_bin_width:
  In km, used in disaggregation calculations to specify the distance bins.
  Example: *distance_bin_width = 20*.
  Default: no default

epsilon_star:
  A boolean controlling the typology of disaggregation output to be provided.
  When True disaggregation is perfomed in terms of epsilon* rather then
  epsilon (see Bazzurro and Cornell, 1999)
  Example: *epsilon_star = true*
  Default: False

extreme_gmv:
  A scalar on an IMT-keyed dictionary specifying when a ground motion value is
  extreme and the engine has to treat is specially.
  Example: *extreme_gmv = 5.0*
  Default: {'default': numpy.inf} i.e. no values are extreme

floating_x_step:
  Float, used in rupture generation for kite faults. indicates the fraction
  of fault length used to float ruptures along strike by the given float
  (i.e. "0.5" floats the ruptures at half the rupture length). Uniform
  distribution of the ruptures is maintained, such that if the mesh spacing
  and rupture dimensions prohibit the defined overlap fraction, the fraction
  is increased until uniform distribution is achieved. The minimum possible
  value depends on the rupture dimensions and the mesh spacing.
  If 0, standard rupture floating is used along-strike (i.e. no mesh nodes
  are skipped).
  Example: *floating_x_step = 0.5*
  Default: 0

floating_y_step:
  Float, used in rupture generation for kite faults. indicates the fraction
  of fault width used to float ruptures down dip. (i.e. "0.5" floats that
  half the rupture length). Uniform distribution of the ruptures
  is maintained, such that if the mesh spacing and rupture dimensions
  prohibit the defined overlap fraction, the fraction is increased until
  uniform distribution is achieved. The minimum possible value depends on
  the rupture dimensions and the mesh spacing.
  If 0, standard rupture floating is used along-strike (i.e. no mesh nodes
  on the rupture dimensions and the mesh spacing.
  Example: *floating_y_step = 0.5*
  Default: 0

ignore_encoding_errors:
  If set, skip characters with non-UTF8 encoding
  Example: *ignore_encoding_errors = true*.
  Default: False

ignore_master_seed:
  If set, estimate analytically the uncertainty on the losses due to the
  uncertainty on the vulnerability functions.
  Example: *ignore_master_seed = vulnerability*.
  Default: None

export_dir:
  Set the export directory.
  Example: *export_dir = /tmp*.
  Default: the current directory, "."

exports:
  Specify what kind of outputs to export by default.
  Example: *exports = csv, rst*.
  Default: empty list

ground_motion_correlation_model:
  Enable ground motion correlation.
  Example: *ground_motion_correlation_model = JB2009*.
  Default: None

ground_motion_correlation_params:
  To be used together with ground_motion_correlation_model.
  Example: *ground_motion_correlation_params = {"vs30_clustering": False}*.
  Default: empty dictionary

ground_motion_fields:
  Flag to turn on/off the calculation of ground motion fields.
  Example: *ground_motion_fields = false*.
  Default: True

gsim:
   Used to specify a GSIM in scenario or event based calculations.
   Example: *gsim = BooreAtkinson2008*.
   Default: "[FromFile]"

hazard_calculation_id:
  Used to specify a previous calculation from which the hazard is read.
  Example: *hazard_calculation_id = 42*.
  Default: None

hazard_curves_from_gmfs:
  Used in scenario/event based calculations. If set, generates hazard curves
  from the ground motion fields.
  Example: *hazard_curves_from_gmfs = true*.
  Default: False

hazard_maps:
  Set it to true to export the hazard maps.
  Example: *hazard_maps = true*.
  Default: False

horiz_comp_to_geom_mean:
  Apply the correction to the geometric mean when possible,
  depending on the GMPE and the Intensity Measure Component
  Example: *horiz_comp_to_geom_mean = true*.
  Default: False

ignore_covs:
  Used in risk calculations to set all the coefficients of variation of the
  vulnerability functions to zero.
  Example *ignore_covs = true*
  Default: False

ignore_missing_costs:
  Accepts exposures with missing costs (by discarding such assets).
  Example: *ignore_missing_costs = nonstructural, business_interruption*.
  Default: False

iml_disagg:
  Used in disaggregation calculations to specify an intensity measure type
  and level.
  Example: *iml_disagg = {'PGA': 0.02}*.
  Default: no default

imt_ref:
  Reference intensity measure type used to compute the conditional spectrum.
  The imt_ref must belong to the list of IMTs of the calculation.
  Example: *imt_ref = SA(0.15)*.
  Default: empty string

individual_rlzs:
  When set, store the individual hazard curves and/or individual risk curves
  for each realization.
  Example: *individual_rlzs = true*.
  Default: False

individual_curves:
  Legacy name for `individual_rlzs`, it should not be used.

infer_occur_rates:
   If set infer the occurrence rates from the first probs_occur in
   nonparametric sources.
   Example: *infer_occur_rates = true*
   Default: False

infrastructure_connectivity_analysis:
    If set, run the infrastructure connectivity analysis.
    Example: *infrastructure_connectivity_analysis = true*
    Default: False

inputs:
  INTERNAL. Dictionary with the input files paths.

intensity_measure_types:
  List of intensity measure types in an event based calculation.
  Example: *intensity_measure_types = PGA SA(0.1)*.
  Default: empty list

intensity_measure_types_and_levels:
  List of intensity measure types and levels in a classical calculation.
  Example: *intensity_measure_types_and_levels={"PGA": logscale(0.1, 1, 20)}*.
  Default: empty dictionary

interest_rate:
  Used in classical_bcr calculations.
  Example: *interest_rate = 0.05*.
  Default: no default

investigation_time:
  Hazard investigation time in years, used in classical and event based
  calculations.
  Example: *investigation_time = 50*.
  Default: no default

job_id:
   ID of a job in the database
   Example: *job_id = 42*.
   Default: 0 (meaning create a new job)

limit_states:
   Limit states used in damage calculations.
   Example: *limit_states = moderate, complete*
   Default: no default

local_timestamp:
  Timestamp that includes both the date, time and the time zone information
  Example: 2023-02-06 04:17:34+03:00
  Default: None

lrem_steps_per_interval:
  Used in the vulnerability functions.
  Example: *lrem_steps_per_interval  = 1*.
  Default: 0

mag_bin_width:
  Width of the magnitude bin used in disaggregation calculations.
  Example: *mag_bin_width = 0.5*.
  Default: 1.

master_seed:
  Seed used to control the generation of the epsilons, relevant for risk
  calculations with vulnerability functions with nonzero coefficients of
  variation.
  Example: *master_seed = 1234*.
  Default: 123456789

max:
  Compute the maximum across realizations. Akin to mean and quantiles.
  Example: *max = true*.
  Default: False

max_blocks:
  INTERNAL. Used in classical calculations

max_data_transfer:
  INTERNAL. Restrict the maximum data transfer in disaggregation calculations.

max_potential_gmfs:
  Restrict the product *num_sites * num_events*.
  Example: *max_potential_gmfs = 1E9*.
  Default: 2E11

max_potential_paths:
  Restrict the maximum number of realizations.
  Example: *max_potential_paths = 200*.
  Default: 15_000

max_sites_correl:
  Maximum number of sites for GMF-correlation.
  Example: *max_sites_correl = 2000*
  Default: 1200

max_sites_disagg:
  Maximum number of sites for which to store rupture information.
  In disaggregation calculations with many sites you may be forced to raise
  *max_sites_disagg*, that must be greater or equal to the number of sites.
  Example: *max_sites_disagg = 100*
  Default: 10

max_weight:
  INTERNAL

maximum_distance:
  Integration distance. Can be give as a scalar, as a dictionary TRT -> scalar
  or as dictionary TRT -> [(mag, dist), ...]
  Example: *maximum_distance = 200*.
  Default: no default

maximum_distance_stations:
  Applies only to scenario calculations with conditioned GMFs to discard
  stations.
  Example: *maximum_distance_stations = 100*.
  Default: None

mean:
  Flag to enable/disable the calculation of mean curves.
  Example: *mean = false*.
  Default: True

minimum_asset_loss:
  Used in risk calculations. If set, losses smaller than the
  *minimum_asset_loss* are consider zeros.
  Example: *minimum_asset_loss = {"structural": 1000}*.
  Default: empty dictionary

minimum_distance:
   If set, distances below the minimum are rounded up.
   Example: *minimum_distance = 5*
   Default: 0

minimum_engine_version:
   If set, raise an error if the engine version is below the minimum
   Example: *minimum_engine_version = 3.22*
   Default: None

minimum_intensity:
  If set, ground motion values below the *minimum_intensity* are
  considered zeros.
  Example: *minimum_intensity = {'PGA': .01}*.
  Default: empty dictionary

minimum_magnitude:
  If set, ruptures below the *minimum_magnitude* are discarded.
  Example: *minimum_magnitude = 5.0*.
  Default: 0

modal_damage_state:
  Used in scenario_damage calculations to export only the damage state
  with the highest probability.
  Example: *modal_damage_state = true*.
  Default: false

mosaic_model:
  Used to restrict the ruptures to a given model
  Example: *mosaic_model = ZAF*
  Default: empty string

num_epsilon_bins:
  Number of epsilon bins in disaggregation calculations.
  Example: *num_epsilon_bins = 3*.
  Default: 1

num_rlzs_disagg:
  Used in disaggregation calculation to specify how many outputs will be
  generated. `0` means all realizations, `n` means the n closest to the mean
  hazard curve.
  Example: *num_rlzs_disagg=1*.
  Default: 0

number_of_ground_motion_fields:
  Used in scenario calculations to specify how many random ground motion
  fields to generate.
  Example: *number_of_ground_motion_fields = 100*.
  Default: no default

number_of_logic_tree_samples:
  Used to specify the number of realizations to generate when using logic tree
  sampling. If zero, full enumeration is performed.
  Example: *number_of_logic_tree_samples = 0*.
  Default: 0

oversampling:
  When equal to "forbid" raise an error if tot_samples > num_paths in classical
  calculations; when equal to "tolerate" do not raise the error (the default).
  Example: *oversampling = forbid*
  Default: tolerate

poes:
  Probabilities of Exceedance used to specify the hazard maps or hazard spectra
  to compute.
  Example: *poes = 0.01 0.02*.
  Default: empty list

poes_disagg:
   Alias for poes.

pointsource_distance:
  Used in classical calculations to collapse the point sources. Can also be
  used in conjunction with *ps_grid_spacing*.
  Example: *pointsource_distance = 50*.
  Default: {'default': 100}

postproc_func:
  Specify a postprocessing function in calculators/postproc.
  Example: *postproc_func = compute_mrd.main*
  Default: 'dummy.main' (no postprocessing)

postproc_args:
  Specify the arguments to be passed to the postprocessing function
  Example: *postproc_args = {'imt': 'PGA'}*
  Default: {} (no arguments)

prefer_global_site_params:
  INTERNAL. Automatically set by the engine.

ps_grid_spacing:
  Used in classical calculations to grid the point sources. Requires the
  *pointsource_distance* to be set too.
  Example: *ps_grid_spacing = 50*.
  Default: 0, meaning no grid

quantiles:
  List of probabilities used to compute the quantiles across realizations.
  Example: quantiles = 0.15 0.50 0.85
  Default: empty list

random_seed:
  Seed used in the sampling of the logic tree.
  Example: *random_seed = 1234*.
  Default: 42

reference_depth_to_1pt0km_per_sec:
  Used when there is no site model to specify a global z1pt0 parameter,
  used in some GMPEs.
  Example: *reference_depth_to_1pt0km_per_sec = 100*.
  Default: no default

reference_depth_to_2pt5km_per_sec:
  Used when there is no site model to specify a global z2pt5 parameter,
  used in some GMPEs.
  Example: *reference_depth_to_2pt5km_per_sec = 5*.
  Default: no default

reference_vs30_type:
  Used when there is no site model to specify a global vs30 type.
  The choices are "inferred" or "measured"
  Example: *reference_vs30_type = measured"*.
  Default: "inferred"

reference_vs30_value:
  Used when there is no site model to specify a global vs30 value.
  Example: *reference_vs30_value = 760*.
  Default: no default

region:
  A list of lon/lat pairs used to specify a region of interest
  Example: *region = 10.0 43.0, 12.0 43.0, 12.0 46.0, 10.0 46.0*
  Default: None

region_grid_spacing:
  Used together with the *region* option to generate the hazard sites.
  Example: *region_grid_spacing = 10*.
  Default: None

return_periods:
  Used in the computation of the loss curves.
  Example: *return_periods = 200 500 1000*.
  Default: empty list.

reqv_ignore_sources:
  Used when some sources in a TRT that uses the equivalent distance term
  should not be collapsed.
  Example: *reqv_ignore_sources = src1 src2 src3*
  Default: empty list

risk_imtls:
  INTERNAL. Automatically set by the engine.

risk_investigation_time:
  Used in risk calculations. If not specified, the (hazard) investigation_time
  is used instead.
  Example: *risk_investigation_time = 50*.
  Default: None

rlz_index:
  Used in disaggregation calculations to specify the realization from which
  to start the disaggregation.
  Example: *rlz_index = 0*.
  Default: None

rupture_dict:
  Dictionary with rupture parameters lon, lat, dep, mag, rake, strike, dip
  Example: *rupture_dict = {'lon': 10, 'lat': 20, 'dep': 10, 'mag': 6, 'rake': 0}*
  Default: {}

rupture_mesh_spacing:
  Set the discretization parameter (in km) for rupture geometries.
  Example: *rupture_mesh_spacing = 2.0*.
  Default: 5.0

sampling_method:
  One of early_weights, late_weights, early_latin, late_latin)
  Example: *sampling_method = early_latin*.
  Default: 'early_weights'

mea_tau_phi:
  Save the mean and standard deviations computed by the GMPEs
  Example: *mea_tau_phi = true*
  Default: False

sec_peril_params:
  INTERNAL

secondary_perils:
  List of supported secondary perils.
  Example: *secondary_perils = HazusLiquefaction, HazusDeformation*
  Default: empty list

ses_per_logic_tree_path:
  Set the number of stochastic event sets per logic tree realization in
  event based calculations.
  Example: *ses_per_logic_tree_path = 100*.
  Default: 1

ses_seed:
  Seed governing the generation of the ground motion field.
  Example: *ses_seed = 123*.
  Default: 42

shakemap_id:
  Used in ShakeMap calculations to download a ShakeMap from the USGS site
  Example: *shakemap_id = usp000fjta*.
  Default: no default

shakemap_uri:
  Dictionary used in ShakeMap calculations to specify a ShakeMap. Must contain
  a key named "kind" with values "usgs_id", "usgs_xml" or "file_npy".
  Example: shakemap_uri = {
  "kind": "usgs_xml",
  "grid_url": "file:///home/michele/usp000fjta/grid.xml",
  "uncertainty_url": "file:///home/michele/usp000fjta/uncertainty.xml"}.
  Default: empty dictionary

shift_hypo:
  Used in classical calculations to shift the rupture hypocenter.
  Example: *shift_hypo = true*.
  Default: false

sites:
  Used to specify a list of sites.
  Example: *sites = 10.1 45, 10.2 45*.

site_labels:
  Specify a dictionary label_string -> label_index assuming each site
  have a field "label" corresponding to the label index.
  Example: *site_labels = {"Cascadia": 1}*.
  Default: {}

tile_spec:
  INTERNAL

tiling:
  Used to force the tiling or non-tiling strategy in classical calculations
  Example: *tiling = true*.
  Default: None, meaning the engine will decide what to do

smlt_branch:
   Used to restrict the source model logic tree to a specific branch
   Example: *smlt_branch=b1*
   Default: empty string, meaning all branches

soil_intensities:
  Used in classical calculations with amplification_method = convolution

source_id:
   Used for debugging purposes. When given, restricts the source model to the
   given source IDs.
   Example: *source_id = src001 src002*.
   Default: empty list

source_nodes:
  INTERNAL

spatial_correlation:
  Used in the ShakeMap calculator. The choics are "yes", "no" and "full".
  Example: *spatial_correlation = full*.
  Default: "yes"

specific_assets:
  INTERNAL

split_sources:
  INTERNAL

split_by_gsim:
  INTERNAL

outs_per_task:
  How many outputs per task to generate (honored in some calculators)
  Example: *outs_per_task = 3*
  Default: 4

std:
  Compute the standard deviation  across realizations. Akin to mean and max.
  Example: *std = true*.
  Default: False

steps_per_interval:
  Used in the fragility functions when building the intensity levels
  Example: *steps_per_interval = 4*.
  Default: 1

tectonic_region_type:
   Used to specify a tectonic region type.
   Example: *tectonic_region_type = Active Shallow Crust*.
   Default: '*'

time_event:
  Used in scenario_risk calculations when the occupancy depend on the time.
  Valid choices are "avg", "day", "night", "transit".
  Example: *time_event = day*.
  Default: "avg"

time_per_task:
  Used in calculations with task splitting. If a task slice takes longer
  then *time_per_task* seconds, then spawn subtasks for the other slices.
  Example: *time_per_task=1000*
  Default: 600

total_losses:
  Used in event based risk calculations to compute total losses and
  and total curves by summing across different loss types. Possible values
  are "structural+nonstructural", "structural+contents",
  "nonstructural+contents", "structural+nonstructural+contents".
  Example: *total_losses = structural+nonstructural*
  Default: None

truncation_level:
  Truncation level used in the GMPEs.
  Example: *truncation_level = 0* to compute median GMFs.
  Default: no default

uniform_hazard_spectra:
  Flag used to generated uniform hazard specta for the given poes
  Example: *uniform_hazard_spectra = true*.
  Default: False

use_rates:
  When set, convert to rates before computing the statistical hazard curves
  Example: *use_rates = true*.
  Default: False

vs30_tolerance:
  Used when amplification_method = convolution.
  Example: *vs30_tolerance = 20*.
  Default: 0

width_of_mfd_bin:
  Used to specify the width of the Magnitude Frequency Distribution.
  Example: *width_of_mfd_bin = 0.2*.
  Default: None

with_betw_ratio:
  Specify the between ratio for GSIMs with only the Total StdDev.
  This is necessary in conditioned GMFs calculations.
  Example: *with_betw_ratio = 1.7*
  Default: None
"""