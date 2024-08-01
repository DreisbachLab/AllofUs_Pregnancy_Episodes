# AllofUs_Pregnancy_Episodes

## Access to All of Us Research Program Dataset
This algorithm was built for use in the secure All of Us Research Hub Researcher Workbench, a cloud-based platform that supports analysis of All of Us Research Program data. Individuals interested in analyzing the data must be registered users of the All of Us Program. If you are not registered as an All of Us researcher, please check out the All of Us Research Hub (https://www.researchallofus.org/). Registration includes, 1) confirming your affiliation with an institution that has a Data Use and Registration Agreement (DURA), 2) creating an account and verifying your identity with Login.gov or ID.me, 3) completing the mandatory training focusing on responsible and ethical conduct of research, and 4) signing the Data User Code of Conduct (DUCC). 

## Define Cohort on the AoU Researcher Workbench platform
The cohort for this analysis is all participants assigned female at birth (AFAB). In the AoU platform, under “demographics”, choose “sex assigned at birth”, then select “female”. In the most recent version of the data (Curated Data Repository [CDR] Version 7) the total cohort count is n=249,565. We use the data from the Registered Tier for this analysis. Both the Registered and the Controlled Tiers are appropriate.


## Description of files in this repo
In this repository, we provide 12 files (4 .csv files, 5 .py files, 1 .ipynb file, one license, and one README file) for users who want to identify pregnancy episodes from the All of Us Research Program.

Input data files:

•	Matcho_term_durations.csv contains data related to various pregnancy or medical terms. These term durations can be used to identify health episodes based on their length, categorize them into specific types.
•	PPS_concepts.csv contains data related to specific concepts associated with pregnancy or prenatal care. This file serves as a reference for mapping different prenatal tests and procedures to specific gestational timeframes.
•	Matcho_outcome_limits.csv contains data specifying the minimum number of days required between different pregnancy-related events or categories. This file provides guidelines on the minimum allowable time intervals between various pregnancy-related events.
•	HIP_concepts.csv contains data related to various medical or health concepts that associated with pregnancy. This file helps identifying and categorizing different health concepts related to pregnancy.

Python scripts (.py): 
•	ESD.py aims to handle the extraction and processing of specific data related to timing-based concepts, such as visits, conditions, and other relevant medical records.
•	HIP.py focuses on pulling outcome-based records, particularly those defined by certain concepts related to health information.
•	PPS.py is used to process and analyze pregnancy or prenatal care related data, which involves various medical tests or procedures related to pregnancy.
•	HIP_PPS_Merge.py is responsible for merging data from the HIP and PPS datasets. 
•	HIPPS.py is the main script for the analysis, coordinating the different components (ESD, HIP, PPS) to generate the final dataset.

## Jupyter Notebook (.ipynb) 

•	Full_HIPPS_Episodes.ipynb combines the data from various scripts and files. It performs the final steps of analysis and generate the final csv. data named “HIPPS_Pregnancy_Episodes.csv”

Instruction and license files:
•	README shows specific steps how to identify comprehensive information on pregnancy episodes among people whose sex assigned as “female” at birth. 
•	License describes the terms under which the software and associated documentation files are distributed

## Data dictionaries for these four csv files



## How to Use Files

  1. In your All of Us workspace where you generate the AFAB cohort, select “Analysis” along the top tab.
  2. Open a new Jupyter Notebook.
  3. In the Jupyter ribbon, select File > Open.
  4. In the above header, select 'Upload', and select the files from this repo (4 csv files, 5 py files from the repo, and the ipynb file).
  5. Open the workbook “Full_HIPPS_Episodes.ipynb”.
  6. Run the workbook “Full_HIPPS_Episodes.ipynb”. The full dataset will be saved as a .csv in your directory

![image](https://github.com/user-attachments/assets/65638fd8-3470-452a-beb9-ca3f59179d74)

