from config import step

def main(run_step=1):
    """
    Run the following steps one by one. Check the output files each step to make sure the output values make sense.
    run_step = 1: run LandUse step 1 - prepare land use data
    run_step = 2: run LandUse step 2 - validate input parcels
    run_step = 'A': run SynPop step A - interpolate households and persons by GEOID between two horizon years
    run_step = 'B': run SynPop step B - prepare households for base year or future year using KR oldTAZ COB parcel forecast
    """
    if run_step in [1, 2, 3, 4, 5]:
        from landuse.landuse import LandUse
        landuse = LandUse(run_step=run_step)
    
    # run the first two steps of the land use process
    if run_step == 1:
        landuse.step_1_prepare_land_use()
    elif run_step == 2:
        landuse.step_2_validate_input_parcels()
    
    # switch to the synthetic population process and run all the steps
    if run_step == 'A' or run_step == 'B':
        from synthetic_population.synthetic_population import SynPop
        synpop = SynPop(run_step=run_step)

    if run_step == 'A':
        synpop.step_A_interpolate_hhps()
    elif run_step == 'B':
        synpop.step_B_distribute_hh2parcel()
    
    #!!! 
    # Now run PopulationSim: D:\projects\PopulationSim\PSRCrun0423\run_population_sim.py
    #!!!

    # # run the following steps of the synthetic population process
    # synpop.step_C_parcelization()

    # # switch back to the land use and run the remaining steps
    # landuse.step_3_interpolate_parcel_files()
    # landuse.step_4_sync_population_to_parcel()
    # landuse.step_5_sync_pop2parcels()
    

if __name__ == '__main__':
    main(run_step=step)