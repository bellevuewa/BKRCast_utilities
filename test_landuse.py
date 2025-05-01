from landuse.landuse import LandUse
from synthetic_population.synthetic_population import SynPop

def main():
    landuse = LandUse()
    # run the first two steps of the land use process
    landuse.step_1_prepare_land_use()
    landuse.step_2_validate_input_parcels()
    # switch to the synthetic population process and run all the steps
    synpop = SynPop()
    synpop.step_A_interpolate_hhps()
    synpop.step_B_distribute_hh2parcel()
    
    #!!! 
    # Now run PopulationSim: D:\projects\PopulationSim\PSRCrun0423\run_population_sim.py
    #!!!

    # run the following steps of the synthetic population process
    synpop.step_C_parcelization()

    # move back to the land use and run the remaining steps
    landuse.step_3_interpolate_parcel_files()
    landuse.step_4_sync_population_to_parcel()
    landuse.step_5_sync_pop2parcels()
    

if __name__ == '__main__':
    main()