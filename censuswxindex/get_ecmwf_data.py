import censuswxindex as cwi

# Define directories
ifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs/fc'
ifs_an_dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs/an'
aifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf_aifs/fc'
lsm_dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs/land_sea_mask'

# Define parameters
start_ifs = '2016-01-01'
start_aifs = '2024-03-01'
end = '2024-12-31'
params = {'t2m': '167.128'}
init_times = ['00', '12']
lead_times = ['00', '06', '12', '18', '24', '48', '72', '96', '120', '168', '240']
valid_times = ['00', '06', '12', '18']
grids = ['0.25', '0.125']
bounds = ['90','-180','0','0'] # Northwest Quadrant

# Retrieve data
cwi.retrieve_ifs_forecast(
    target_dir = ifs_fc_dir,
    start = start_ifs,
    end = end,
    grids = grids,
    params = params,
    init_times = init_times,
    lead_times = lead_times,
    bounds = bounds
)

cwi.retrieve_ifs_analysis(
    target_dir = ifs_an_dir,
    start = start_ifs,
    end = end,
    grids = grids,
    params = params,
    times = times,
    bounds = bounds
)

cwi.retrieve_aifs_forecast(
    target_dir = aifs_fc_dir,
    start = start_aifs,
    end = end,
    params = params,
    init_times = init_times,
    lead_times = lead_times,
    bounds = bounds
)

cwi.retrieve_land_sea_mask(
    target_dir = lsm_dir,
    grids = grids,
    bounds = bounds
)