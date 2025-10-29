
import pandas as pd
import numpy as np
import datetime as dt
import os

import utilities
from datetime import datetime

# Pulls timing-related concept IDs

def data_pull(concepts_table):

    concepts = '(' + concepts_table["domain_concept_id"].astype(dtype="str").str.cat(sep=', ') + ')'

    visit_query = """
        with AFAB as (
            select
                person_id
            from """ + os.environ["WORKSPACE_CDR"] + """.person
            where
                sex_at_birth_concept_id = 45878463 

        )

        select distinct
            vis.person_id,
            vis.visit_end_date,
            con.condition_concept_id as concept_id,
            NULL as value_as_number,
            'Condition' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.condition_occurrence con
            on vis.visit_occurrence_id = con.visit_occurrence_id
        where 
            vis.person_id in (select person_id from AFAB)
            and con.condition_concept_id in """ + concepts + """

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            obs.observation_concept_id as concept_id,
            value_as_number,
            'Observation' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.observation obs
            on vis.visit_occurrence_id = obs.visit_occurrence_id
        where 
            vis.person_id in (select person_id from AFAB)
            and obs.observation_concept_id in """ + concepts + """

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            proc.procedure_concept_id as concept_id,
            NULL as value_as_number,
            'Procedure' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.procedure_occurrence proc
            on vis.visit_occurrence_id = proc.visit_occurrence_id
        where 
            vis.person_id in (select person_id from AFAB)
            and proc.procedure_concept_id in """ + concepts + """

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            mes.measurement_concept_id as concept_id,
            value_as_number,
            'Measurement' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.measurement mes
            on vis.visit_occurrence_id = mes.visit_occurrence_id
        where 
            vis.person_id in (select person_id from AFAB)
            and mes.measurement_concept_id in """ + concepts + """



            """

    visit_table = pd.read_gbq(
        visit_query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
        progress_bar_type="tqdm_notebook")
    
    return visit_table


def get_episodes(visit_table,concept_table):

    # Combines visits with min/max month timing and orders by date 
    visits_w_ranges = (
        pd.merge(
            visit_table[['person_id','visit_end_date','concept_id']]
            .assign(visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)),
            concept_table[['domain_concept_id','min_month','max_month']]
            .rename({'domain_concept_id':'concept_id'},axis=1)
        )
        .sort_values(['person_id','visit_end_date','concept_id'])
        .reset_index(drop=True)
        .assign(visit_rank = lambda x : (
            x
            .assign(temp = 1)
            .groupby('person_id')['temp']
            .cumsum()
        )
               )
    )
    
    # Compares visits with all previous ones
    # Get max and min diff in gestation timing, compares with actual time difference 
    get_diffs = (
        pd.merge(
            visits_w_ranges,
            visits_w_ranges
            .rename({
                'visit_end_date' : 'prev_visit',
                'concept_id' : 'prev_concept',
                'min_month' : 'prev_min',
                'max_month' : 'prev_max',
                'visit_rank' : 'prev_rank'
            },axis=1),
            how='left'
        )
        .query('visit_rank > prev_rank or (visit_rank == 1 and prev_rank == 1)')
        .reset_index(drop=True)
        .assign(
            t_diff = lambda x : (x.visit_end_date - x.prev_visit).dt.days/30,
            max_diff = lambda x : x.max_month - x.prev_min + 2,
            min_diff = lambda x : x.min_month - x.prev_max - 2,
            agree = lambda x : (x.t_diff <= x.max_diff) & (x.t_diff >= x.min_diff),
            bridge_val = lambda x : x.visit_rank - x.prev_rank
        )
    )
    
    # Consecuitively checks if visit agrees with previous visits
    # Flags and numbers start of new episodes
    get_eps = (
        get_diffs
        .groupby(['person_id','visit_end_date','concept_id'])[['agree']]
        .sum()
        .reset_index()
        .assign(
            t_diff = lambda x : (x.visit_end_date - (
                x
                .groupby('person_id')['visit_end_date']
                .shift(1))).dt.days/30,
            new_ep = lambda x : ((x.agree == 0) & (x.t_diff > 2)) | (x.t_diff.isnull()) | (x.t_diff > 10),
            episode = lambda x : (
                x
                .groupby('person_id')['new_ep']
                .cumsum()
            )
        )[['person_id','visit_end_date','concept_id','episode']]
    )
    
    # Flags episode to remove if length is not feasible
    remove_eps = (
        pd.merge(
            get_eps,
            get_eps
            .groupby(['person_id','episode'])['visit_end_date']
            .agg(['min','max'])
            .reset_index()
            .rename({'min' : 'ep_min','max' : 'ep_max'},axis=1)
            .assign(
                ep_len = lambda x : (x.ep_max - x.ep_min).dt.days/30
            )
        )
        .query('ep_len > 12')[['person_id','episode']]
        .drop_duplicates()
        .rename({'episode' : 'rem_ep'},axis=1)
    )
    
    # Remove flagged episodes, and decrement subsequent episodes
    rem_eps = (
        pd.merge(
            get_eps.drop(['episode'],axis=1),
            pd.merge(
                get_eps,
                remove_eps,
                how = 'left'
            )
            .query('rem_ep.isnull() or episode != rem_ep')
            .assign(
                dec = lambda x : x.episode > x.rem_ep,
            )
            .groupby(['person_id','visit_end_date','concept_id','episode'])[['dec']]
            .sum()
            .reset_index()
            .assign(
                episode = lambda x : x.episode - x.dec
            )[['person_id','visit_end_date','concept_id','episode']],
            how = 'inner'
        )
    )
    
    return rem_eps

def get_range(episodes):

    # Get recorded range for each episode
    ep_range = (
        episodes
        .groupby(['person_id','episode'])['visit_end_date']
        .agg(['min','max'])
        .reset_index()
        .rename({'min' : 'ep_min','max' : 'ep_max'},axis=1)
        .assign(ep_max_plus_two = lambda x : x.ep_max + pd.to_timedelta(60,'days'))
    )
    
    # Flag episodes where episode freuqncy is too great
    freq_too_high = (
        pd.merge(
            ep_range
            .groupby('person_id')['episode']
            .max()
            .reset_index(),
            pd.merge(
                ep_range
                .groupby(['person_id'])[['ep_min']]
                .min()
                .reset_index(),        
                ep_range
                .groupby(['person_id'])[['ep_max']]
                .max()
                .reset_index()
            )
        )
        .assign(
            rng = lambda x : (x.ep_max - x.ep_min).dt.days/365,
            freq = lambda x : x.episode / x.rng
        )
        .query('freq >= 5 and episode > 1')['person_id']
    )
    
    # Remove episodes where episode freuqncy is too great
    final_episodes = (
        pd.merge(
            episodes,
            ep_range[~ep_range['person_id'].isin(freq_too_high)]
        )
    )
    
    return final_episodes

def label_trimester_events(final_episodes_df, visit_table):
    # Ensure date columns are datetime
    final_episodes_df['visit_end_date'] = pd.to_datetime(final_episodes_df['visit_end_date'])
    final_episodes_df['ep_min'] = pd.to_datetime(final_episodes_df['ep_min'])
    final_episodes_df['ep_max'] = pd.to_datetime(final_episodes_df['ep_max'])
    visit_table['visit_end_date'] = pd.to_datetime(visit_table['visit_end_date'])

    # Merge on person_id, visit_end_date, and concept_id
    merged = pd.merge(
        final_episodes_df,
        visit_table[['person_id', 'visit_end_date', 'concept_id', 'value_as_number', 'Concept_Domain']],
        on=['person_id', 'visit_end_date', 'concept_id'],
        how='left'
    )

    # Rename for consistency with trimester labeling functions
    merged = merged.rename(columns={
        'visit_end_date': 'clinical_visit_end_date',
        'ep_min': 'estimated_start_date',
        'ep_max': 'pregnancy_end_date'
    })

    # Drop rows with missing essential dates
    merged.dropna(subset=['estimated_start_date', 'pregnancy_end_date', 'clinical_visit_end_date'], inplace=True)

    # Filter to visits within the episode window
    within_episode = merged[
        (merged['clinical_visit_end_date'] >= merged['estimated_start_date']) &
        (merged['clinical_visit_end_date'] <= merged['pregnancy_end_date'])
    ].copy()
    
    # Compute gestational day
    within_episode['gest_day'] = (within_episode['clinical_visit_end_date'] - within_episode['estimated_start_date']).dt.days

    # Assign trimester
    def get_trimester(day):
        if day <= 90:
            return "Trimester 1"
        elif day <= 195:
            return "Trimester 2"
        else:
            return "Trimester 3"

    within_episode['trimester'] = within_episode['gest_day'].apply(get_trimester)

    # Detailed trimester events
    trimester_events = within_episode[[
        'person_id',
        'episode',
        'concept_id',
        'Concept_Domain',
        'clinical_visit_end_date',
        'gest_day',
        'trimester'
    ]].rename(columns={'clinical_visit_end_date': 'event_date'})

    # Summary: counts per trimester per episode
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

def main(concepts_file):
    
    concepts_table = pd.read_csv(concepts_file)
    
    visits = data_pull(concepts_table)
    
    eps = get_episodes(visits,concepts_table)
    
    final_episodes = get_range(eps)
    
    # Trimester integration
    #

    PPS_trimester_events, PPS_trimester_summary = label_trimester_events(final_episodes, visits)
    
    PPS_trimester_event_details = utilities.summarize_trimester_events_by_concept(PPS_trimester_events)
    
    # Save PPS trimester information to CSV
    PPS_trimester_events.to_csv(f"PPS_Trimester_Events.csv", index=False)
    PPS_trimester_summary.to_csv(f"PPS_Trimester_Summary.csv", index=False)
    PPS_trimester_event_details.to_csv(f"PPS_Trimester_Event_Details.csv", index=False)
    
    return final_episodes, PPS_trimester_events, PPS_trimester_summary, PPS_trimester_event_details
