import os
import pandas as pd
import numpy as np

'''
This file contains helper functions for labeling pregnancy-related events,
pulling survey/demographic data, and calculating derived measures such as
trimester summaries, parity, delivery method, and concordance scores.

'''

# Label visit events based on trimester
def label_trimester_events(episodes_df, visit_table):
    # Ensure datetime types
    episodes_df['estimated_start_date'] = pd.to_datetime(episodes_df['estimated_start_date'])
    episodes_df['visit_end_date'] = pd.to_datetime(episodes_df['visit_end_date']) # When pregnancy ended, ex: delivery, loss, etc. 'x'
    visit_table['visit_end_date'] = pd.to_datetime(visit_table['visit_end_date']) # When a clinical event ended, ex: lab, diagnosis, etc. 'y' 

    # Merge clinical visits with pregnancy episode start/end
    merged = pd.merge(
        visit_table,
        episodes_df[['person_id', 'episode', 'estimated_start_date', 'visit_end_date']],
        on='person_id',
        how='left'
    )
    
    # Clarify overlapping visit_end_date columns
    merged = merged.rename(columns={
    'visit_end_date_x': 'clinical_visit_end_date',
    'visit_end_date_y': 'pregnancy_end_date'
    })
    
    merged['pregnancy_end_date'] = pd.to_datetime(merged['pregnancy_end_date'])

    # Keep events that fall within the episode window
    within_episode = merged[
        (merged['clinical_visit_end_date'] >= merged['estimated_start_date']) &
        (merged['clinical_visit_end_date'] <= merged['pregnancy_end_date'])
    ].copy()

    # Calculate gestational day relative to episode start
    within_episode['gest_day'] = (within_episode['clinical_visit_end_date'] - within_episode['estimated_start_date']).dt.days

    # Assign trimester based on gestational day
    def get_trimester(day):
        if day <= 90:                  # ~12 weeks, 6 days
            return "Trimester 1"
        elif day <= 195:               # ~27 weeks, 6 days
            return "Trimester 2"
        else:
            return "Trimester 3"

    within_episode['trimester'] = within_episode['gest_day'].apply(get_trimester)

    # Return raw trimester events
    trimester_events = within_episode[[
        'person_id',
        'episode',
        'concept_id',
        'Concept_Domain',
        'clinical_visit_end_date',
        'gest_day',
        'trimester'
    ]].rename(columns={'clinical_visit_end_date': 'event_date'})

    # Return aggregated trimester summary per episode
    trimester_summary = (
        trimester_events
        .groupby(['person_id', 'episode', 'trimester'])
        .size()
        .reset_index(name='event_count')
        .pivot(index=['person_id', 'episode'], columns='trimester', values='event_count')
        .fillna(0)
        .reset_index()
    )

    return trimester_events, trimester_summary

# Summarize trimester events by concept/domain
def summarize_trimester_events_by_concept(trimester_events):
    detailed_summary = (
        trimester_events
        .groupby(['person_id', 'episode', 'trimester', 'concept_id', 'Concept_Domain'])
        .size()
        .reset_index(name='event_count')
        .sort_values(by=['person_id', 'episode', 'trimester', 'event_count'], ascending=[True, True, True, False])
    )
    return detailed_summary

# Pull pregnancy status survey responses (pregnancy confirmation)
def pull_pregnancy_status(person_ids):

    ids_string = ', '.join(str(int(x)) for x in person_ids)

    query = f"""
    SELECT
        person_id,
        value_as_concept_id AS pregnancy_status_concept_id,
        observation_datetime AS pregnancy_survey_datetime
    FROM `{os.environ["WORKSPACE_CDR"]}.observation`
    WHERE observation_source_concept_id = 1585811
      AND value_as_concept_id = 4299535    
      AND person_id IN ({ids_string})
    """

    preg_status = pd.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ)
    )
    
    preg_status['pregnancy_survey_datetime'] = pd.to_datetime(preg_status['pregnancy_survey_datetime']).dt.date

    return preg_status

# Flag episodes where survey confirms pregnancy within episode window
def flag_currently_pregnant(singles, preg_status_df):

    singles['merged_start'] = pd.to_datetime(singles['merged_start'])
    singles['merged_end'] = pd.to_datetime(singles['merged_end'])
    preg_status_df['pregnancy_survey_datetime'] = pd.to_datetime(preg_status_df['pregnancy_survey_datetime'])

    singles_with_survey = singles.merge(
        preg_status_df[['person_id', 'pregnancy_survey_datetime']],
        on='person_id',
        how='left'
    )

    # Flag if survey date falls inside merged episode window
    singles_with_survey['survey_confirmed_pregnancy'] = (
        (singles_with_survey['pregnancy_survey_datetime'] >= singles_with_survey['merged_start']) &
        (singles_with_survey['pregnancy_survey_datetime'] <= singles_with_survey['merged_end'])
    )

    # Default False if no match
    singles_with_survey['survey_confirmed_pregnancy'] = singles_with_survey['survey_confirmed_pregnancy'].fillna(False)
    
    # Drop survey date if not inside window
    singles_with_survey.loc[~singles_with_survey['survey_confirmed_pregnancy'], 'pregnancy_survey_datetime'] = pd.NaT
    
    return singles_with_survey

# Pull smoking survey data (latest per person)
def pull_smoking_status(person_ids):
    
    ids_string = ', '.join(str(int(x)) for x in person_ids)

    query = f"""
    SELECT
        person_id,
        value_as_number AS smoking_years,
        observation_datetime AS smoking_survey_datetime
    FROM `{os.environ["WORKSPACE_CDR"]}.observation`
    WHERE observation_source_concept_id = 1585873
      AND person_id IN ({ids_string})
    """

    smoking_status = pd.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ)
    )

    # Keep most recent record per person
    smoking_status = (
        smoking_status
        .sort_values(['person_id', 'smoking_survey_datetime'])
        .drop_duplicates('person_id', keep='last')
        .reset_index(drop=True)
    )
    
    smoking_status['smoking_survey_datetime'] = pd.to_datetime(smoking_status['smoking_survey_datetime']).dt.date

    return smoking_status

# Merge smoking status into episode dataframe
def merge_smoking_status(singles, smoking_status):
    
    singles = (
        singles
        .merge(smoking_status, how='left', on='person_id')
    )
    
    return singles

# Pull demographics for DOB and calculate age at episode end
def pull_demographics(person_ids):
    ids_string = ', '.join(str(int(x)) for x in person_ids)

    query = f"""
    SELECT
        person_id,
        birth_datetime AS date_of_birth
    FROM `{os.environ["WORKSPACE_CDR"]}.person`
    WHERE person_id IN ({ids_string})
    """

    demographics = pd.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ)
    )

    demographics['date_of_birth'] = pd.to_datetime(demographics['date_of_birth']).dt.date

    return demographics

# Add age_at_delivery (based on merged_end - DOB)
def add_age_at_merged_end(singles_df, demographics_df):

    demographics_df['date_of_birth'] = pd.to_datetime(demographics_df['date_of_birth'])
    singles_df['merged_end'] = pd.to_datetime(singles_df['merged_end'])

    merged = singles_df.merge(
        demographics_df[['person_id', 'date_of_birth']],
        on='person_id',
        how='left'
    )

    # Age calculation (accounting for birthdays not yet reached)
    def calculate_age(row):
        dob = row['date_of_birth']
        end = row['merged_end']
        if pd.isnull(dob) or pd.isnull(end):
            return np.nan
        age = end.year - dob.year
        
        if (end.month, end.day) < (dob.month, dob.day):
            age -= 1
        return age

    merged['age_at_delivery'] = merged.apply(calculate_age, axis=1)
    merged.drop(['date_of_birth'], axis=1, inplace=True)

    return merged

# Add parity (max episode number per person)
def add_parity_column(singles_df):
    
    parity_lookup = (
        singles_df.groupby('person_id')['episode'].max().reset_index()
        .rename(columns={'episode': 'parity'})
    )

    singles_with_parity = singles_df.merge(
        parity_lookup,
        on='person_id',
        how='left'
    )

    # Keep parity column next to episode
    cols = singles_with_parity.columns.tolist()
    if 'episode' in cols and 'parity' in cols:
        cols.remove('parity')
        episode_idx = cols.index('episode')
        cols.insert(episode_idx + 1, 'parity')
        singles_with_parity = singles_with_parity[cols]

    return singles_with_parity

# Pull delivery concept IDs from 'Delivery_Method_Map_07232025.csv' sourced from workbench database
# All concept IDs pulled explicity or implicitly indicate vaginal delivery or c-section
def pull_delivery_method(person_ids):

    delivery_map = pd.read_csv("Delivery_Method_Map_07232025.csv")
    
    # Get concept_ids grouped by domain
    condition_ids = delivery_map.loc[delivery_map['occurrence'] == 'condition', 'concept_id'].unique()
    observation_ids = delivery_map.loc[delivery_map['occurrence'] == 'observation', 'concept_id'].unique()
    procedure_ids = delivery_map.loc[delivery_map['occurrence'] == 'procedures', 'concept_id'].unique()
    
    ids_string = ', '.join(str(int(x)) for x in person_ids)
    
    # Build queries for condition/observation/procedure
    condition_query = f"""
      SELECT
        person_id,
        condition_concept_id AS concept_id,
        condition_start_date AS date,
        'condition' AS occurrence
      FROM `{os.environ["WORKSPACE_CDR"]}.condition_occurrence`
      WHERE condition_concept_id IN ({','.join(map(str, condition_ids))})
        AND person_id IN ({ids_string})
    """
    
    observation_query = f"""
      SELECT
        person_id,
        observation_concept_id AS concept_id,
        observation_date AS date,
        'observation' AS occurrence
      FROM `{os.environ["WORKSPACE_CDR"]}.observation`
      WHERE observation_concept_id IN ({','.join(map(str, observation_ids))})
        AND person_id IN ({ids_string})
    """
    
    procedure_query = f"""
      SELECT
        person_id,
        procedure_concept_id AS concept_id,
        procedure_date AS date,
        'procedures' AS occurrence
      FROM `{os.environ["WORKSPACE_CDR"]}.procedure_occurrence`
      WHERE procedure_concept_id IN ({','.join(map(str, procedure_ids))})
        AND person_id IN ({ids_string})
    """
    
    query = f"""
      {condition_query}
      UNION ALL
      {observation_query}
      UNION ALL
      {procedure_query}
    """
    
    delivery_method_df = pd.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ)
    )

    # Merge back mapped delivery method labels
    delivery_method_df = delivery_method_df.merge(
        delivery_map[['concept_id', 'delivery_method']],
        how='left',
        on='concept_id'
    )

    return delivery_method_df

# Merge delivery method into episodes (±30 days of merged_end)
def merge_delivery_method(singles, delivery_method_df):
    singles = singles.copy()
    singles['merged_start'] = pd.to_datetime(singles['merged_start'])
    singles['merged_end'] = pd.to_datetime(singles['merged_end'])
    delivery_method_df = delivery_method_df.copy()
    delivery_method_df['date'] = pd.to_datetime(delivery_method_df['date'])

    singles['merged_end_plus_30'] = singles['merged_end'] + pd.Timedelta(days=30)

    # Cross join on same person_id
    singles['_key'] = singles['person_id']
    delivery_method_df['_key'] = delivery_method_df['person_id']

    merged = singles.merge(
        delivery_method_df,
        on='_key',
        suffixes=('_single', '_delivery'),
        how='left'
    )

    # Match window: merged_start <= date <= merged_end + 30
    merged = merged[
        (merged['date'] >= merged['merged_start']) &
        (merged['date'] <= merged['merged_end_plus_30'])
    ]

    # If multiple matches, keep closest to merged_end
    merged['date_diff'] = (merged['date'] - merged['merged_end']).abs()
    merged = merged.sort_values(['person_id_single', 'merged_end', 'date_diff'])
    merged = merged.drop_duplicates(['person_id_single', 'merged_end'], keep='first')

    # Merge back onto singles
    singles = singles.merge(
        merged[['person_id_single', 'merged_end', 'delivery_method']],
        left_on=['person_id', 'merged_end'],
        right_on=['person_id_single', 'merged_end'],
        how='left'
    )

    singles = singles.drop(columns=['merged_end_plus_30', '_key', 'person_id_single'])

    return singles

# Compute concordance score (0–2 scale)
def calculate_concordance_score(final_episodes):
    final_episodes = final_episodes.copy()

    # Ensure datetimes
    final_episodes['HIP_end'] = pd.to_datetime(final_episodes['HIP_end'])
    final_episodes['PPS_outcome_date'] = pd.to_datetime(final_episodes['PPS_outcome_date'])
    
    # Calculate if PPS and HIP outcomes are within ±14 days
    final_episodes['date_diff_days'] = (final_episodes['HIP_end'] - final_episodes['PPS_outcome_date']).abs().dt.days
    final_episodes['date_within_14'] = final_episodes['date_diff_days'] <= 14
    
    # Check if gestational age falls within expected range
    final_episodes['GA_plausible'] = (
        (final_episodes['gestational_age'] >= final_episodes['min_term']) &
        (final_episodes['gestational_age'] <= final_episodes['max_term'])
    )
    
    # Assign points based on outcome match and date within ±14 days
    final_episodes['outcome_points'] = final_episodes.apply(
        lambda row: 1 if row['outcome_match'] and row['date_within_14'] else 0,
        axis = 1
    )
    
    final_episodes['GA_points'] = final_episodes['GA_plausible'].astype(int)
    
    # Total score = outcome match (0/1) + GA plausibility (0/1)
    final_episodes['outcome_concordance_score'] = final_episodes['outcome_points'] + final_episodes['GA_points']
    
    # Drop intermediate calculation columns
    final_episodes = final_episodes.drop(
        columns=['date_diff_days', 'date_within_14', 'GA_plausible', 'outcome_points', 'GA_points']
    )
        
    return final_episodes
