from config import step

def main(run_step=1):
    """
    Run the following steps one by one. Check the output files each step to make sure the output values make sense.
    Go to *landuse process*:
        run_step = 1: run LandUse step 1 - prepare land use data
        run_step = 2: run LandUse step 2 - validate input parcels
    Switch to the *synthetic population process*:
        run_step = 'A': run SynPop step A - interpolate households and persons by GEOID between two horizon years
        run_step = 'B': run SynPop step B - prepare households for base year or future year using KR oldTAZ COB parcel forecast
    Then go to D:\projects\PopulationSim\PSRCrun0423 to run PopulationSim: run_populationsim.py, and 
    copy synthetic_persons.csv and synthetic_households.csv outputs to the SynPop working dir (rename if needed)
        run_step = 'C': run SynPop step C - parcelizationV3
    Switch back to *landuse process*:
        run_step = 3: run LandUse step 3 - interpolate parcel files between PSRC bookends
        run_step = 4: run LandUse step 4 - replace parcel columns with required data columns
        run_step = 5: run LandUse step 5 - sync population to parcels
    """
    if run_step in [1, 2, 3, 4, 5]:
        from landuse.landuse import LandUse
        landuse = LandUse(run_step=run_step)
    
    # run the first two steps of the land use process
    if run_step == 1:
        landuse.step_1_prepare_land_use()
    elif run_step == 2:
        landuse.step_2_validate_input_parcels()
    elif run_step == 3:
        landuse.step_3_interpolate_parcel_files()
    elif run_step == 4:
        landuse.step_4_update_parcel_columns()
    elif run_step == 5:
        landuse.step_5_sync_pop2parcels()
    
    # switch to the synthetic population process and run all the steps
    if run_step in ['A', 'B', 'C']:
        from synthetic_population.synthetic_population import SynPop
        synpop = SynPop(run_step=run_step)

    if run_step == 'A':
        synpop.step_A_interpolate_hhps()
    elif run_step == 'B':
        synpop.step_B_distribute_hh2parcel()
    elif run_step == 'C':
        synpop.step_C_parcelization()
    

if __name__ == '__main__':
    main(run_step=step)