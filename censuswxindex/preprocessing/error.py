def calculate_absolute_error(fc_dir, an_dir, clim_path, save_dir, start, end, lead_times):
    '''
    Calculates the root mean squared error between forecast and analysis data for given lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    return