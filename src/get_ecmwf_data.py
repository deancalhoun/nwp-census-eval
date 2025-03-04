import preprocessing

dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs'
preprocessing.retrieve_ifs_analysis('/'.join([dir, "an"]), start='2024-01-01', end='2024-12-01')

dir = '/glade/derecho/scratch/dcalhoun/ecmwf_aifs'
preprocessing.retrieve_aifs_forecast(dir)