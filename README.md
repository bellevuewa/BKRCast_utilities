# BKRCast_utilities

## Land Use and Synthetic Population Workflow
This repository contains scripts and tools for **processing land use data** and generating synthetic populations to support urban planning, transportation modeling, and demographic forecasting, where the process also **converts data from SoundCast to BKRCast**.

## Overview
This project provides a multi-step workflow as listed below.

- Preparing land use data at the parcel level
- Validating prepared parcel-level data
- Interpolating household and person data between bookends
- Allocating households to individual parcels
- Running PopulationSim to generate synthetic populations
- Interpolating parcel data based on population synthesis across horizon years
- Converting interpolated data for use in the BKRCast model
- Aligning parcel-level households with the synthetic population data

## Repository Structure
.

├── landuse/ # Land use data processing scripts

├── synthetic_population/ # Synthetic population generation scripts

├── config.py # Example configuration

├── main.py # Main orchestrator script

└── README.md

**Note**: The `populationsim/` module is maintained separately in its own folder and is not included directly in this repo.

## Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone https://github.com/bellevuewa/BKRCast_utilities.git
   cd BKRCast_utilities
   ```

2. **Install required dependencies**:
    - Set up your environment by following [BKRCast set-up steps](https://github.com/bellevuewa/BKRCast/wiki/Setup-for-OpenPath)
    - Install [PopulationSim](https://github.com/RSGInc/populationsim?tab=readme-ov-file) from [their documentation](https://activitysim.github.io/populationsim/)

3. **Configure the paths and settings**:
Edit config.py to set paths to your initial, the specific step desired to run, input/output paths, target years, and other parameters.

## Usage
Run the workflow one step at a time, ensuring the outputs from each step are valid before proceeding to the next. 

### Quick Start
Specify the desired step in `config.py` along with input/output paths, and other parameters, then execute the pipeline by running `main.py`. 

**Note**: Refer to `main.py` for detailed usage instructions and examples.