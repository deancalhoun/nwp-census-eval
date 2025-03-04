import preprocessing

dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs'
# # finish 00/168
# preprocessing.retrieve_ifs_forecast('/'.join([dir, "fc"]), start='2024-09-01', end='2024-12-01', init_times = ["00"], lead_times = ["168"])
# # all 00/240
# preprocessing.retrieve_ifs_forecast('/'.join([dir, "fc"]), start='2024-01-01', end='2024-12-01', init_times = ["00"], lead_times = ["240"])
# # finish 12/all lead times
# preprocessing.retrieve_ifs_forecast('/'.join([dir, "fc"]), start='2024-01-01', end='2024-12-01', init_times = ["12"])
# get all analysis
preprocessing.retrieve_ifs_analysis('/'.join([dir, "an"]), start='2024-01-01', end='2024-12-01')