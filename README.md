# Full HIPPS Episodes AOU Research 
Final version for HIPPS_Pregnancy_Episodes, updated from June-September, 2025.

Repo last updated on Oct 28, 2025.
Code last updated on Sep 11, 2025

## Access to All of Us Research Program Dataset

This algorithm was built for use in the secure All of Us Research Hub Researcher Workbench, a cloud-based platform that supports analysis of All of Us Research Program data. Individuals interested in analyzing the data must be registered users of the All of Us Program. If you are not registered as an All of Us researcher, please check out the All of Us Research Hub (https://www.researchallofus.org/). Registration includes, 1) confirming your affiliation with an institution that has a Data Use and Registration Agreement (DURA), 2) creating an account and verifying your identity with Login.gov or ID.me, 3) completing the mandatory training focusing on responsible and ethical conduct of research, and 4) signing the Data User Code of Conduct (DUCC).

This work is an extension of the algorithm developed by Jones et al to define pregnancy episodes where participants are pregnant in the National COVID Cohort Collaborative (N3C). Both the N3C data and the All of Us Research Program are mapped to the Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM). Because they share the CDM, it was possible to replicate the algorithm and transfer it into Pandas, rather than relying on Spark which is not available in the All of Us Researcher Workbench. Users interested in this algorithm can use the scripts to identify pregnancy episodes outside of the All of Us Research Workbench where a Spark environment is unavailable as long as the data are mapped to OMOP.

## Define Cohort on the AoU Researcher Workbench platform

The cohort for this analysis is all participants assigned female at birth (AFAB). In the AoU platform, under “demographics”, choose “sex assigned at birth”, then select “female”. In the most recent version of the data (Curated Data Repository [CDR] Version 7) the total cohort count is n=249,565. We use the data from the Registered Tier for this analysis. Both the Registered and the Controlled Tiers are appropriate.

## Requirements & Instructions

When selecting data version, **use: All of Us Controlled Tier Dataset v7**.

In your All of Us workspace where you generate the AFAB cohort, select “Analysis” along the top tab.
Open a new Jupyter Notebook.

In the Jupyter ribbon, select File > Open.

In the above header, select 'Upload', and select the files from this repo (5 .csv files, 6 .py files, and 1 .ipynb file from the repo).

Open the workbook “Full_HIPPS_Episodes.ipynb”.

Run the single, master notebook “Full_HIPPS_Episodes.ipynb”. 

Full dataset of pregnancy episodes & datasets for trimester information will be saved as .csv files in your directory.

## Description of Files
In this repository, we provide 10 files (5 .csv files, 6 .py files, 1 .ipynb file) for users who want to identify pregnancy episodes from the All of Us Research Program.

**Input data files (.csv)**:

• `Matcho_term_durations.csv` contains data related to various pregnancy or medical terms. These term durations can be used to identify health episodes based on their length, and categorize them into specific types.

• `PPS_concepts.csv` contains data related to specific concepts associated with pregnancy or prenatal care. This file serves as a reference for mapping different prenatal tests and procedures to specific gestational timeframes.

• `Matcho_outcome_limits.csv` contains data specifying the minimum number of days required between different pregnancy-related events or categories. This file provides guidelines on the minimum allowable time intervals between various pregnancy-related events.

• `HIP_concepts.csv` contains data related to various medical or health concepts that are associated with pregnancy. This file helps identify and categorize different health concepts related to pregnancy.

• `Deliver_Method_Map_07232025.csv` contains data related to specific concepts utilized to extract information regarding delivery methods explicitly and implicitly indicating vaginal or cesarean section delivery. (Last udpated on July 23rd, 2025).

**Python scripts (.py)**:

• `utilities.py` contains additional functions utilized in `HIP.py`, `PPS.py`, `HIP_PPS_Merge.py` to handle trimester information and extract data from the singles dataframe for .

• `ESD.py` aims to handle the extraction and processing of specific data related to timing-based concepts, such as visits, conditions, and other relevant medical records.

• `HIP.py` focuses on pulling outcome-based records, particularly those defined by certain concepts related to health information.

• `PPS.py` is used to process and analyze pregnancy or prenatal care related data, which involves various medical tests or procedures related to pregnancy.

• `HIP_PPS_Merge.py` is responsible for merging data from the HIP and PPS datasets.

• `HIPPS.py` is the main script for the analysis, coordinating the different components (ESD, HIP, PPS) to generate the final dataset.

**Jupyter Notebook (.ipynb)**

• `Full_HIPPS_Episodes.ipynb` combines the data from various scripts and files. It performs the final steps of analysis and generate the final csv. Data named “HIPPS_Pregnancy_Episodes.csv”

## Expected Output Files

**1. Extracted Trimester Data:**

*a. trimester_events:*

• Granularity: One row per individual clinical event (not aggregated)

• Purpose: Raw labeled data — each event tagged with a trimester

• Ex: Clinical event during which trimester

*b. trimester_summary:*

• Granularity: One row per person–episode, aggregated by trimester

• Purpose: Summary count of total events per trimester

• Ex: In each episode, how many clinical events per trimester
    
*c. trimester_event_details:*

• Granularity: One row per concept per trimester per episode

• Purpose: Detailed summary showing what occurred and how often, grouped

• Ex: How many clinical events per person's concept domain visit per trimester

• `HIP_Trimester_Events.csv`

• `HIP_Trimester_Summary.csv`

• `HIP_Trimester_Event_Details.csv`

• `PPS_Trimester_Events.csv`

• `PPS_Trimester_Summary.csv`

• `PPS_Trimester_Event_Details.csv`

**2. Extracted Full HIPPS Data**

*Contains a comprehensive overview of pregnancy episodes.*

• `HIPPS_Pregnancy_Episodes.csv` 

## Understanding Outcome Concordance Score

Derived from Jones, et al.’s *Who is Pregnant?* research paper. 

Score ranges from 0~2:

• 0 = no concordance (neither outcome/timing nor gestational plausibility align)

• 1 = partial concordance (either outcome/timing or gestational plausibility aligns)

• 2 = strong concordance (both align)

## Data Dictionary for Final Output CSV File
• `Data Dictionary_New.csv` contains description for each column head.

## Instruction and license files:

• README shows specific steps how to identify comprehensive information on pregnancy episodes among people whose sex assigned as “female” at birth.

• License describes the terms under which the software and associated documentation files are distributed

## Simple Flow Chart
<img width="911" height="536" alt="Image" src="https://github.com/user-attachments/assets/dbb7c37d-53eb-4028-87c8-2fd557d8621a" />

## References

Jones, et al. https://github.com/jonessarae/n3c_pregnancy_cohort

Jones SE, Bradwell KR, Chan LE, et al. Who is pregnant? Defining real-world data-based pregnancy episodes in the National COVID Cohort Collaborative (N3C). JAMIA Open. 2023;6(3):ooad067. Published 2023 Aug 16. doi:10.1093/jamiaopen/ooad067

Smith LH, Wang W, Keefe-Oates B. Pregnancy episodes in All of Us: harnessing multi-source data for pregnancy-related research. J Am Med Inform Assoc. Published online July 24, 2024. doi:10.1093/jamia/ocae195
