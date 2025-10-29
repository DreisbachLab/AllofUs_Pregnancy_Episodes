#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import os

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

def main(concepts_file):
    
    concepts_table = pd.read_csv(concepts_file)
    
    visits = data_pull(concepts_table)
    
    eps = get_episodes(visits,concepts_table)
    
    final_episodes = get_range(eps)
    
    return final_episodes

