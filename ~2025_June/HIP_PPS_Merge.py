#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
import os

# Pull all prgenancy outcomes
def outcome_pull(concepts_table):

    concepts = '(' + concepts_table["concept_id"].astype(dtype="str").str.cat(sep=', ') + ')'

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

def get_PPS_outcomes(PPS_eps,HIP_concept_file,PPS_concept_file):
        
    HIP_concepts = pd.read_csv(HIP_concept_file)
    PPS_concepts = pd.read_csv(PPS_concept_file)

    # Get all concepts with particular outcome
    outcome_concepts = (
        HIP_concepts[['concept_id','concept_name','category']]
        .query("category != 'PREG'")
        .reset_index(drop=True)
    )

    outcome_concept_ids = outcome_concepts['concept_id']

    # Get window for each episode to search for outcome
    wind_dates = (
        pd.merge(
            pd.merge(
                PPS_eps,
                PPS_eps[['person_id','episode','ep_min','ep_max','ep_max_plus_two']]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(next_ep = lambda x : 
                        (
                            x
                            .groupby(['person_id'])['ep_min']
                            .shift(-1)
                        ) - pd.to_timedelta(1,unit='days')
                       )[['person_id','episode','next_ep']],
                how='left'
            ),
            pd.merge(
                PPS_eps,
                PPS_concepts[['domain_concept_id','min_month','max_month']]
                .rename({'domain_concept_id': 'concept_id'},axis=1)
            )[['person_id','episode','concept_id','visit_end_date','min_month','max_month']]
            .sort_values(by=['person_id','episode','visit_end_date','max_month','min_month'],
                         ascending=[True,True,False,False,False])
            .groupby(['person_id','episode'])
            .first()
            .reset_index()
            .assign(
                max_preg_date = lambda x : (
                    x.visit_end_date + pd.to_timedelta((11-x.min_month.astype(int))*30,unit='days')
                )
            )[['person_id','episode','max_preg_date']]
        )
        .assign(
            look_ahead = lambda x : x[['next_ep','max_preg_date']].min(axis=1),
            look_back = lambda x : x.ep_max + pd.to_timedelta(-14,unit='days')
        )
        .drop(['next_ep','max_preg_date'],axis=1)
    )

    # Establish hierarchy order for outcomes
    outcome_order = {
        'LB' : 0,
        'SB' : 1,
        'ECT' : 2,
        'SA' : 3,
        'AB' : 4,
        'DELIV' : 5
    }

    # Get outcome records and dates to match with PPS episodes
    visit_table = (
        pd.merge(
            outcome_pull(outcome_concepts),
            outcome_concepts[['concept_id','category']]
        )[['person_id','visit_end_date','category']]
        .assign( outcome_order = lambda x : x.category.apply(lambda y : outcome_order[y]),
                visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)
               )
        .rename({'visit_end_date' : 'outcome_date'},axis=1)
        .sort_values(['person_id','outcome_date','outcome_order'])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Get outcomes for each PPS episode
    PPS_w_outcomes = (
        pd.merge(
            wind_dates,
            pd.merge(
                wind_dates,
                visit_table
            )[['person_id','episode','look_back','look_ahead','outcome_date','category','outcome_order']]
            .query('outcome_date.isnull() or (look_back <= outcome_date and look_ahead >= outcome_date)')
            .sort_values(['person_id','episode','outcome_order','outcome_date'])
            .groupby(['person_id','episode'])
            .first()
            .reset_index()[['person_id','episode','outcome_date','category']],
            how='left'
        )
    )
    
    return PPS_w_outcomes

def merge_eps(HIP_eps,PPS_w_outcomes):
    
    # Merge HIP and PPS identified episodes
    combine = (
        pd.merge(
            HIP_eps
            .assign(
                visit_end_date = lambda x : pd.to_datetime(x.visit_end_date),
                estimated_start_date = lambda x : pd.to_datetime(x.estimated_start_date)
            )
            .rename(
                {
                    'visit_end_date' : 'HIP_end',
                    'estimated_start_date' : 'HIP_start',
                    'episode' : 'HIP_episode',
                    'outcome_preg_category' : 'HIP_category'
                }
                ,axis=1),
            PPS_w_outcomes[['person_id','episode','ep_min','ep_max','ep_max_plus_two','category','outcome_date']]
            .drop_duplicates()
            .rename(
                {
                    'episode' : 'PPS_episode',
                    'category' : 'PPS_category',
                    'outcome_date' : 'PPS_outcome_date'
                }
                ,axis=1),
            how = 'outer'
        )
    )

    # Join episodes from each algorithm
    # Filter to those with overlap or patients with episodes from only one or the other
    both_or_one = (
        combine
        .query(
            "(HIP_start.isnull() or ep_min.isnull())"
            "or (HIP_start == ep_min and HIP_end == ep_max_plus_two)"
            "or (HIP_start < ep_min and HIP_end > ep_max_plus_two)"
            "or (HIP_start > ep_min and HIP_end < ep_max_plus_two)"
            "or (HIP_start >= ep_min and HIP_start <= ep_max_plus_two)"
            "or (HIP_start <= ep_min and HIP_end >= ep_min)"
            "or (HIP_end >= ep_min and HIP_end <= ep_max_plus_two)"
            "or (HIP_start <= ep_max_plus_two and HIP_end >= ep_max_plus_two)"
        )
        .assign(
            merged_start = lambda x : x[['HIP_start','ep_min']].min(axis=1),
            merged_end = lambda x : x[['HIP_end','ep_max']].max(axis=1)
        )
    )

    # Get HIP only episodes
    HIP_no_PPS = (
        pd.merge(
            HIP_eps
            .assign(
                visit_end_date = lambda x : pd.to_datetime(x.visit_end_date),
                estimated_start_date = lambda x : pd.to_datetime(x.estimated_start_date)
            )
            .rename(
                {
                    'visit_end_date' : 'HIP_end',
                    'estimated_start_date' : 'HIP_start',
                    'episode' : 'HIP_episode',
                    'outcome_preg_category' : 'HIP_category'
                }
                ,axis=1),
            both_or_one[['person_id','HIP_episode']]
            .query('~HIP_episode.isnull()')
            .assign(drp = True),
            how='left'
        )
        .query('drp.isnull()')
        .drop(['drp'],axis=1)
        .assign(
            PPS_episode = np.NaN,
            ep_min = np.NaN,
            ep_max = np.NaN,
            ep_max_plus_two = np.NaN,
            PPS_category = np.NaN,
            merged_start = lambda x : x.HIP_start,
            merged_end = lambda x : x.HIP_end
               )
    )

    # Get PPS only episodes
    PPS_no_HIP = (
        pd.merge(
            PPS_w_outcomes[['person_id','episode','ep_min','ep_max','ep_max_plus_two','category']]
            .drop_duplicates()
            .rename(
                {
                    'episode' : 'PPS_episode',
                    'category' : 'PPS_category'
                }
                ,axis=1),
            both_or_one[['person_id','PPS_episode']]
            .query('~PPS_episode.isnull()')
            .assign(drp = True),
            how='left'
        )
        .query('drp.isnull()')
        .drop(['drp'],axis=1)
        .assign(
            HIP_episode = np.NaN,
            HIP_start = np.NaN,
            HIP_end = np.NaN,
            HIP_category = np.NaN,
            merged_start = lambda x : x.ep_min,
            merged_end = lambda x : x.ep_max
               )
    )

    # Combine all episodes, get merged length
    merged_eps = (
        pd.concat(
            [
                both_or_one,
                HIP_no_PPS,
                PPS_no_HIP
            ]
        )
        .sort_values(['person_id','merged_end','HIP_episode','PPS_episode'])
        .reset_index(drop=True)
        .assign(merged_length = lambda x : (x.merged_end - x.merged_start).apply(lambda y : y.days))
    )
    
    return merged_eps

def remove_duplicates(merged_eps):

    # Find episodes from each algorithm that match with multiple from another
    dups = (
        pd.merge(
            pd.merge(
                merged_eps,
                merged_eps
                .groupby(['person_id','HIP_episode','episode_length'])
                .count()
                .max(axis=1)
                .reset_index()
                .assign(
                    HIP_dup = lambda x : x[0].apply( lambda y : y > 1)
                )[['person_id','HIP_episode','episode_length','HIP_dup']],
                how='left'
            ),
            merged_eps
            .groupby(['person_id','PPS_episode','episode_length'])
            .count()
            .max(axis=1)
            .reset_index()
            .assign(
                PPS_dup = lambda x : x[0].apply( lambda y : y > 1)
            )[['person_id','PPS_episode','episode_length','PPS_dup']],
            how='left'
        )
        .assign(
            HIP_dup = lambda x : x.HIP_dup.fillna(False),
            PPS_dup = lambda x : x.PPS_dup.fillna(False)
        )
    )

    duplicated = (dups[dups['HIP_dup'] | dups['PPS_dup']])
    
    # Keep only non-duplicated episodes
    singles = (
        (dups[~dups['HIP_dup'] & ~dups['PPS_dup']])
        .drop(['HIP_dup','PPS_dup'],axis=1)
        .reset_index(drop=True)
        .sort_values(['person_id','HIP_episode','PPS_episode'])
        .assign(
            episode = lambda x : 
            x
            .assign(tmp = 1)
            .groupby(['person_id'])['tmp']
            .cumsum()
        )
    )
    
    return singles

def main(HIP_eps,PPS_eps,HIP_concept_file,PPS_concept_file):
    
    PPS_w_outcomes = get_PPS_outcomes(PPS_eps,HIP_concept_file,PPS_concept_file)
    
    merged_eps = merge_eps(HIP_eps,PPS_w_outcomes)
    
    singles = remove_duplicates(merged_eps)
    
    return singles

