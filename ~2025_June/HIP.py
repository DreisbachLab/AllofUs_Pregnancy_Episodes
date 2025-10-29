#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import os

# Pulls outcome-based records with concepts defined in Matcho et al
# Member base used is those assigned female sex at birth
#
# For each domain, visit table is joined to domain table to pull
# visit_end_date, concept ID, concept domain, and value for observation or measurement
#

def data_pull(concepts_table):
    
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


# Given an outcome category and outcome limits file, pull records correspodning
# to that outcome and merge episodes when within limit defined in file
#

def define_outcome(all_visits,concept_frame,outcomes,outcome_limits):
    
    # Concept IDs for outcome category
    outcome_concepts = concept_frame[concept_frame["category"].isin(outcomes)][['concept_id','category']]
    
    # Filter to concept ID list, get distinct person/date/category combos
    outcome_visits = (
        all_visits
        .set_index('concept_id')
        .join(outcome_concepts.set_index('concept_id'),how='inner')
        .reset_index()
        .drop(labels=['concept_id','Concept_Domain','value_as_number'],axis=1)
        .drop_duplicates()
        .sort_values(by=["person_id","visit_end_date"])
        .reset_index(drop=True)
        .rename(columns={'category' : 'outcome_preg_category'})
    )
    
    # Get days to previous record
    outcome_visits['days'] = (
        outcome_visits
        .groupby(by='person_id')[['visit_end_date']]
        .transform(lambda x: x - x.shift(1))
        .rename(columns={'visit_end_date' : 'days'})
    )
    
    # Get category of previous record
    outcome_visits['first_preg_category'] = (
        outcome_visits
        .groupby(by='person_id')[['outcome_preg_category']]
        .shift(1,fill_value = '')
        .rename(columns={'outcome_preg_category' : 'first_preg_category'})
    )
    
    # filter to first events or events spaced far enough from previous events
    outcome_episodes = (
        pd.merge(outcome_visits,outcome_limits,how='left')
        .query('days.isnull() or (days.dt.days >= min_days)')
        .drop(labels = ['days','first_preg_category','min_days'],axis=1)
        .reset_index(drop=True)

    )
    
    # Resulting rows are distinct episodes within category
    return outcome_episodes

# Add new outcome episodes to another dataframe, using outcome limits file
# Outcomes from first file are prioritized, enforcing the hierarchy of HIP
#

def add_outcome(first,new,outcome_limits):
    
    # Union of tables
    merged = (
        pd.concat([first,new])
        .sort_values(by=["person_id","visit_end_date"])

    )

    # Get days to previous record
    merged['prev_days'] = (
            merged
            .groupby(by='person_id')[['visit_end_date']]
            .transform(lambda x: x - x.shift(1))
            .rename(columns={'visit_end_date' : 'days'})
        )

    # Get outcome of previous record
    merged['first_preg_category'] = (
        merged
        .groupby(by='person_id')[['outcome_preg_category']]
        .shift(1,fill_value = '')
        .rename(columns={'outcome_preg_category' : 'first_preg_category'})
    )

    # Get days to next record
    merged['next_days'] = (
            merged
            .groupby(by='person_id')[['visit_end_date']]
            .transform(lambda x:  x.shift(-1) - x)
            .rename(columns={'visit_end_date' : 'next_days'})
        )

    # Get outcome of next record
    merged['next_preg_category'] = (
        merged
        .groupby(by='person_id')[['outcome_preg_category']]
        .shift(-1,fill_value = '')
        .rename(columns={'outcome_preg_category' : 'next_preg_category'})
    )

    # Relabel outcome limits columns
    outcome_limits_next = (
        outcome_limits
        .rename(columns={
            'first_preg_category' : 'outcome_preg_category',
            'outcome_preg_category' : 'next_preg_category',
            'min_days' : 'next_min_days',
                })
    )
    
    # Gets records from new outcome that are outside appropriate time frame
    to_add = (
        pd.merge(
            pd.merge(merged[merged['outcome_preg_category'].isin(new['outcome_preg_category'].unique())],
                     outcome_limits,how='left'),
            outcome_limits_next,how='left')
        .query('prev_days.isnull() or (prev_days.dt.days >= min_days)')
        .query('next_days.isnull() or (next_days.dt.days >= next_min_days)')
        .drop(labels = ['prev_days','first_preg_category','min_days',
                       'next_days','next_preg_category','next_min_days'],axis=1)
        .reset_index(drop=True)
    )

    # Add new episodes from new outcome to first table
    return pd.concat([first,to_add]).reset_index(drop=True).sort_values(by=['person_id','visit_end_date'])

# Calculate start date based on minimum and maximum term duration for outcome category

def calc_startdate(episodes,term_durations):

    return (
        pd.merge(
            episodes,
            term_durations.rename(columns={'category' : 'outcome_preg_category'}),
            how='inner'
        )
        .assign(
            min_start_date = lambda x: pd.to_datetime(x.visit_end_date)-pd.to_timedelta(x.max_term,unit='d'),
            max_start_date = lambda x: pd.to_datetime(x.visit_end_date)-pd.to_timedelta(x.min_term,unit='d')
        )
    )

# Using gestation week concepts, define distinct episodes 

def get_gest(all_visits,concept_frame,min_days=70,buffer_days=28):
    
    # Pull gestation week records using gestation concept IDs
    # Use either observation/measurment value or gest vlaue from file
    # Multiply by 7 to get gestation days from week
    gest_values = (
        pd.merge(all_visits,concept_frame,how='inner')
        .query('~gest_value.isnull() or ~value_as_number.isnull()')
        .assign(gest_week = lambda x: x.gest_value.combine_first(x.value_as_number))
        .drop(labels=['concept_id','value_as_number','Concept_Domain','concept_name','gest_value'],axis=1)
        .query('gest_week >= 0 and gest_week <= 44')
        .sort_values(['person_id','visit_end_date'])
        .groupby(['person_id','visit_end_date','category'])
        .agg('max')
        .reset_index()
        .assign(gest_day = lambda x: x.gest_week*7)
    )

    # Find previous record's week value
    gest_values['prev_week'] = (
        gest_values
        .groupby('person_id')['gest_week']
        .shift(1)
    )

    # Find previous record's date
    gest_values['prev_date'] = (
        gest_values
        .groupby('person_id')['visit_end_date']
        .shift(1)
    )

    # Get difference in gest values and dates
    #
    # New diff value is 1 when record is at earlier gest time than previous record
    # and date difference is less than minimum day count. Else it is the week diff
    #
    # New diff 2 value is -1 when date diff is greater than gestation day difference
    # and week difference is positive, else it is new diff
    gest_diffs = (
        gest_values
        .assign(
            week_diff = lambda x:  x.gest_week-x.prev_week,
            day_diff = lambda x: (x.gest_week-x.prev_week)*7 + buffer_days,
            date_diff = lambda x: (x.visit_end_date - x.prev_date).dt.days
        )
        .assign(
            new_diff = lambda x: np.where((x.date_diff < min_days) & (x.week_diff <= 0),1,x.week_diff) 
        )
        .assign(
            new_diff2 = lambda x: np.where((x.date_diff >= x.day_diff) & (x.week_diff > 0),-1,x.new_diff) 
        )
    )

    # Ranks records for each person by ascending date
    gest_diffs['ind'] = (
        gest_diffs    
        .assign(visit_end_date = lambda x: pd.to_datetime(x.visit_end_date))
        .groupby('person_id')['visit_end_date']
        .rank(ascending='True')
    )
    
    # Assign episode number. New episode is either first record or where new_diff2 < 0 
    gest_diffs['episode'] = (
        gest_diffs    
        .assign(episode = lambda x: ((x.new_diff2 <= 0) | (x.ind == 1)))
        .groupby('person_id')['episode']
        .cumsum()
    )

    # Get min/max recorded date by episode 
    gest_episodes = (
        gest_diffs
        .groupby(["person_id","episode"])
        .agg({'visit_end_date' : [np.min,np.max], 'gest_week' : [np.min,np.max]})
        .reset_index()
    )

    # Fix column names, give min/max date and min/max gest week by episode
    gest_episodes.columns = gest_episodes.columns.get_level_values(0)
    gest_episodes.columns = [gest_episodes.columns[0], 
                              gest_episodes.columns[1],
                              'min_gest_date',
                              'max_gest_date',
                              'min_gest_week',
                              'max_gest_week']

    # Work back from min/max date and gest week to get min/max start date
    gest_episodes[['min_gest_start_date','max_gest_start_date']] = (
        gest_episodes
        .assign(
            min_gest_start_date = lambda x : pd.to_datetime(x.min_gest_date) - pd.to_timedelta(x.min_gest_week*7,unit='d'),        
            max_gest_start_date = lambda x : pd.to_datetime(x.max_gest_date) - pd.to_timedelta(x.max_gest_week*7,unit='d')
        )[['min_gest_start_date','max_gest_start_date']]
    )    
    
    return gest_episodes.assign(
        gest_date_diff = lambda x : (
            pd.to_datetime(x.max_gest_start_date) - pd.to_datetime(x.min_gest_start_date)
        ).dt.days
    )

# Combine gestation identified episodes with outcome identified episodes

def add_gestation(start_date_table,gest_ep_table,buffer_days=28):

    # Combine episodes on person id
    merged = pd.merge(start_date_table,gest_ep_table,how='outer')

    # Get episodes of patients with no gestation episodes
    no_gest = (
        merged[merged['episode'].isnull()]
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
    )

    # Get episodes of patients with no outcome episodes
    no_outcome = (
        merged[merged['outcome_preg_category'].isnull()]
        .sort_values(['person_id','min_gest_start_date'])
        .reset_index(drop=True)
        .assign(
            visit_end_date = lambda x : x.max_gest_date,
            outcome_preg_category = 'PREG'
        )
    )

    # Filter to patients with both outcome and gestation records
    # Flag combinations of outcome and gestation records with any sort of overlap
    both = (
        merged[(~merged['episode'].isnull()) & (~merged['outcome_preg_category'].isnull())]
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True).assign(
            gest_start_after_outcome_start = lambda x : (
                x.max_gest_start_date >= (pd.to_datetime(x.min_start_date) - pd.to_timedelta(buffer_days,unit='d'))
            ),
            days_diff = lambda x : (pd.to_datetime(x.visit_end_date) - pd.to_datetime(x.max_gest_date)).dt.days,
            gest_end_before_outcome = lambda x : (
                pd.to_datetime(x.max_gest_date) <= (pd.to_datetime(x.visit_end_date) + pd.to_timedelta(buffer_days,unit='d'))
            ),
            gest_end_after_outcome = lambda x : (
                pd.to_datetime(x.max_gest_date) >= (pd.to_datetime(x.visit_end_date) + pd.to_timedelta(buffer_days,unit='d'))
            ),
            gest_end_after_outcome_start = lambda x : (
                pd.to_datetime(x.max_gest_date) >= (pd.to_datetime(x.min_start_date))
            ),
            gest_start_before_outcome = lambda x : (
                pd.to_datetime(x.max_gest_start_date) <= (pd.to_datetime(x.visit_end_date))
            ),
            gest_start_before_outcome_start = lambda x : (
                pd.to_datetime(x.max_gest_start_date) <= (pd.to_datetime(x.min_start_date) - pd.to_timedelta(buffer_days,unit='d'))
            )
        )
        .assign(
            within_outcome = lambda x : (
                x.gest_start_after_outcome_start 
                & x.gest_end_before_outcome
            ),
            overlap_outcome_end = lambda x : (
                x.gest_start_after_outcome_start
                & x.gest_start_before_outcome
                & x.gest_end_after_outcome
            ),
            overlap_outcome_start = lambda x : (
                x.gest_start_before_outcome_start
                & x.gest_end_after_outcome_start
                & x.gest_end_before_outcome
            ),
            within_gestation = lambda x : (
                (pd.to_datetime(x.max_gest_start_date) <= pd.to_datetime(x.min_start_date))
                & (pd.to_datetime(x.max_gest_date) >= pd.to_datetime(x.visit_end_date))
            ) 
        )
        .assign(
            outcome_gest = lambda x : (
                x.within_outcome |
                x.overlap_outcome_start |
                x.overlap_outcome_end |
                x.within_gestation
            )
        )
        .drop(labels=['gest_start_before_outcome_start',
                      'gest_start_after_outcome_start',
                      'gest_start_before_outcome',
                      'gest_end_after_outcome_start',
                      'gest_end_before_outcome',
                      'gest_end_after_outcome',
                      'within_outcome',
                      'overlap_outcome_start',
                      'overlap_outcome_end',
                      'within_gestation'
                     ],
              axis=1
             )
    )

    # Get overlapping gestation and outcome episodes
    outcome_gest = (
        both[both['outcome_gest']]
        .reset_index(drop=True)
        .drop(labels=['outcome_gest'],axis=1)
    )

    # Get gestation episodes that don't overlap with outcome episodes
    gest_only = (
        pd.merge(
            both,
            (
                outcome_gest[['person_id','max_gest_date']]
                .drop_duplicates()
                .assign(chk = 1)
            ),
            how='left'
        )
        .query('chk.isnull()')
        .drop(labels=['chk','outcome_gest'],axis=1)
        .assign(
            visit_end_date = lambda x: x.max_gest_date,
            outcome_preg_category = 'PREG',
            max_term = np.NaN,
            min_term = np.NaN,
            retry = np.NaN,
            min_start_date = np.NaN,
            max_start_date = np.NaN,
            days_diff = np.NaN
        )
        .drop_duplicates()
    )

    # Get outcome episodes that don't iverlap with gest episodes
    outcome_only = (
        pd.merge(
            both,
            (
                outcome_gest[['person_id','visit_end_date']]
                .drop_duplicates()
                .assign(chk = 1)
            ),
            how='left'
        )
        .query('chk.isnull()')
        .drop(labels=['chk','outcome_gest'],axis=1)
        .assign(
            episode = np.NaN,
            min_gest_date = np.NaN,
            max_gest_date = np.NaN,
            min_gest_week = np.NaN,
            max_gest_week = np.NaN,
            min_gest_start_date = np.NaN,
            max_gest_start_date = np.NaN,
            gest_date_diff = np.NaN,
            days_diff = np.NaN
        )
        .drop_duplicates()
    )

    # Combine the 5 different groups of episodes
    add_gest = (
        pd.concat([
            no_outcome.assign(days_diff = np.NaN),
            no_gest.assign(days_diff = np.NaN),
            outcome_gest,
            gest_only,
            outcome_only
        ]
        )
        .drop(labels=['episode'],axis=1)
        .drop_duplicates()
        .sort_values(by=['person_id','visit_end_date'])
        .reset_index(drop=True)
    )

    # Number episodes
    add_gest['episode'] = (
        add_gest
        .assign(
            visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)
        )
        .groupby('person_id')['visit_end_date']
        .rank(ascending='True')
    ).astype('int')
    
    return add_gest

# Removes duplicate episodes, reclassifies those whose time exceeds the 
# outcome term limits and removes episodes of negative length
#

def clean_episodes(episodes_w_gest,buffer_days=28):

    # Identifies and drop duplicte episodes
    # Flags length relative to outcome term limits
    drop_dups = (
        pd.merge(
            episodes_w_gest,
            pd.merge(
                episodes_w_gest,
                episodes_w_gest
                .assign(days_diff = lambda x : abs(x.days_diff))
                .groupby(['person_id','max_gest_date'])[['visit_end_date','days_diff']]
                .agg({'visit_end_date' : 'count', 'days_diff' : 'min'})
                .query("visit_end_date > 1")
                .reset_index()
                .drop(labels=['visit_end_date'],axis=1)
                .rename(columns={'days_diff' : 'days_diff_chk'}),
                how = 'left'
            )
            .query('(days_diff_chk == days_diff) or days_diff_chk.isnull()')
            .assign(days_diff = lambda x : abs(x.days_diff))
            .groupby(['person_id','visit_end_date'])[['max_gest_date','days_diff']]
            .agg({'max_gest_date' : 'count', 'days_diff' : 'min'})
            .query("max_gest_date > 1")
            .reset_index()
            .drop(labels=['max_gest_date'],axis=1)
            .rename(columns={'days_diff' : 'days_diff_chk'}),
            how = 'left'
        )
        .query('(days_diff_chk == days_diff) or days_diff_chk.isnull()')
        .drop(labels=['days_diff_chk'],axis=1)
        .reset_index(drop=True)
        .assign(
            under_max = lambda x : (
                ((pd.to_datetime(x.visit_end_date) - x.max_gest_start_date).dt.days <= x.max_term)
                | x.max_gest_start_date.isnull()
            ),
            over_min = lambda x : (
                ((pd.to_datetime(x.visit_end_date) - x.max_gest_start_date).dt.days >= x.min_term)
                | x.max_gest_start_date.isnull()
            )
        )
    )

    # Remove outcome info and reclassify episodes with length outside term limits
    over_under = (
        pd.concat([
            drop_dups
            .query("(under_max and over_min) or outcome_preg_category == 'PREG'")
            .assign(
                removed_category = np.NaN,
                removed_outcome = False
            ),
            drop_dups
            .query("(~under_max or ~over_min) and outcome_preg_category != 'PREG'")
            .assign(
                removed_category = lambda x : x.outcome_preg_category,
                removed_outcome = True,
                outcome_preg_category = 'PREG',
                visit_end_date = lambda x : x.max_gest_date,
                max_term = np.NaN,
                min_term = np.NaN,
                retry = np.NaN,
                min_start_date = np.NaN,
                max_start_date = np.NaN
            )
        ])
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
    )

    # Remove episodes of negative length
    neg_days = (
        pd.concat([
            over_under
            .query("outcome_preg_category == 'PREG' or days_diff.isnull() or days_diff >= " + str(buffer_days) + "*-1"),
            over_under
            .query("outcome_preg_category != 'PREG' and ~days_diff.isnull() and days_diff < " + str(buffer_days) + "*-1")
            .assign(
                removed_category = lambda x : x.outcome_preg_category,
                removed_outcome = True,
                outcome_preg_category = 'PREG',
                visit_end_date = lambda x : x.max_gest_date,
                max_term = np.NaN,
                min_term = np.NaN,
                retry = np.NaN,
                min_start_date = np.NaN,
                max_start_date = np.NaN
            )
        ])
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
        .assign(
            gest_at_outcome = lambda x : (pd.to_datetime(x.visit_end_date) - x.max_gest_start_date).dt.days
        )
    )

    neg_days['episode'] = (
        neg_days
        .assign(
            visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)
        )
        .groupby('person_id')['visit_end_date']
        .rank(ascending='True')
    ).astype('int')


    return neg_days
    

# Finds episodes that overlap or occur too quickly in succession and removes excess

def remove_overlaps(clean_episodes):

    cleaned = clean_episodes
    
    cleaned['prev_date'] = (
        cleaned
        .groupby('person_id')['visit_end_date']
        .shift(1)
    )

    cleaned['prev_cat'] = (
        cleaned
        .groupby('person_id')['outcome_preg_category']
        .shift(1)
    )

    cleaned['prev_retry'] = (
        cleaned
        .groupby('person_id')['retry']
        .shift(1)
    )

    cleaned['prev_gest'] = (
        cleaned
        .groupby('person_id')['max_gest_date']
        .shift(1)
    )

    get_overlap = (
        cleaned
        .assign(
            prev_date_diff = lambda x : 
            ((pd.to_datetime(x.max_gest_start_date) - pd.to_datetime(x.prev_date)).dt.days).combine_first(
                (pd.to_datetime(x.min_start_date) - pd.to_datetime(x.prev_date)).dt.days
            )
        )
        .assign(
            has_overlap = lambda x : x.prev_date_diff < 0
        )
    )

    drop_overlap = (
        pd.merge(
            get_overlap,
            get_overlap
            .query("has_overlap and prev_cat == 'PREG'")[['person_id','prev_gest','prev_cat']]
            .rename(columns = {'prev_gest' : 'max_gest_date','prev_cat' : 'outcome_preg_category'})
            .assign(chk = 1),
            how = 'left'
        )
        .query('chk.isnull()')
        .drop(labels=['chk'],axis=1)
        .assign(
            estimated_start_date = lambda x : np.where(
                (x.has_overlap & ~x.prev_retry.isnull()),
                pd.to_datetime(x.prev_date) + pd.to_timedelta(x.prev_retry,unit='d'),
                np.where(
                    x.max_gest_start_date.isnull(),
                    x.min_start_date,
                    x.max_gest_start_date
                )
            )
        )
        .assign(
            gest_at_outcome = lambda x : (pd.to_datetime(x.visit_end_date) - x.estimated_start_date).dt.days,
            under_max = lambda x : (pd.to_datetime(x.visit_end_date) - x.estimated_start_date).dt.days <= x.max_term,
            over_min = lambda x : (pd.to_datetime(x.visit_end_date) - x.estimated_start_date).dt.days >= x.min_term
        )
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
    )

    drop_overlap['episode'] = (
            drop_overlap
            .assign(
                visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)
            )
            .groupby('person_id')['visit_end_date']
            .rank(ascending='True')
    ).astype('int')

    drop_overlap['prev_date_2'] = (
        drop_overlap
        .groupby('person_id')['visit_end_date']
        .shift(1)
    )

    drop_overlap_2 = (
        drop_overlap
        .assign(
            prev_date_diff_2 = lambda x : ((pd.to_datetime(x.estimated_start_date) - pd.to_datetime(x.prev_date_2)).dt.days)
        )
        .assign(
            has_overlap_2 = lambda x : x.prev_date_diff_2 < 0
        )
    )

    drop_under = (
        pd.concat([
            drop_overlap_2
            .query("over_min or outcome_preg_category == 'PREG' or max_gest_week.isnull()"),
            drop_overlap_2
            .query("~over_min and outcome_preg_category != 'PREG' and ~max_gest_week.isnull()")
            .assign(
                removed_category = lambda x : x.outcome_preg_category,
                removed_outcome = True,
                outcome_preg_category = 'PREG',
                visit_end_date = lambda x : x.max_gest_date,
                max_term = np.NaN,
                min_term = np.NaN,
                retry = np.NaN,
                min_start_date = np.NaN,
                max_start_date = np.NaN
            )
        ])
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
    )

    return drop_under

# Gets cleaned episodes with gestation info to determine estimated start date

def final_episodes(drop_overlaps,gest_episodes):

    eps_w_gest = (
        pd.merge(
            drop_overlaps[[
                'person_id',
                'outcome_preg_category',
                'visit_end_date',
                'estimated_start_date',
                'episode'
            ]]
            .drop_duplicates(),
            gest_episodes[['person_id','min_gest_date','min_gest_week']],
            how='left'
        )
        .assign(
            chk = lambda x : (
                ~x.min_gest_date.isnull() 
                & (pd.to_datetime(x.estimated_start_date) <= pd.to_datetime(x.min_gest_date)) 
                & (pd.to_datetime(x.visit_end_date) >= pd.to_datetime(x.min_gest_date))
            )
        )
        .query('chk')
        .drop(labels=['chk'],axis=1)
        .drop_duplicates()
    )

    final = (
        pd.concat([
            pd.merge(
                drop_overlaps[[
                    'person_id',
                    'outcome_preg_category',
                    'visit_end_date',
                    'estimated_start_date',
                    'episode'
                ]]
                .drop_duplicates(),
                eps_w_gest[['person_id','visit_end_date']]
                .assign(
                    chk = 1
                ),
                how='left'
            )
            .query('chk.isnull()')
            .assign(
                min_gest_date = np.NaN,
                min_gest_week = np.NaN
            ),
            eps_w_gest
        ])
        .sort_values(['person_id','visit_end_date'])
        .reset_index(drop=True)
        .assign(fill = 1)
        .assign(
            episode_length = lambda x : ((pd.to_datetime(x.visit_end_date) - pd.to_datetime(x.min_gest_date)).dt.days).combine_first(x.fill)
        )
        .assign(
            episode_length = lambda x : (np.where(x.episode_length == 0,1,x.episode_length)).astype('int')
        )
        .drop(labels=['min_gest_date','min_gest_week','fill','chk'],axis=1)
    )
    
    return final


# Runs all together
# Takes concept, outcome limits and term duration file names as input
def main(concept_file,outcome_file,durations_file):    

    HIP_concepts = pd.read_csv(concept_file)
    Matcho_outcome_limits = pd.read_csv(outcome_file)
    Matcho_term_durations = pd.read_csv(durations_file)

    visit_table = data_pull(HIP_concepts)

    sb_episodes = define_outcome(visit_table,HIP_concepts,['SB'],Matcho_outcome_limits)

    lb_episodes = define_outcome(visit_table,HIP_concepts,['LB'],Matcho_outcome_limits)

    ect_episodes = define_outcome(visit_table,HIP_concepts,['ECT'],Matcho_outcome_limits)

    deliv_episodes = define_outcome(visit_table,HIP_concepts,['DELIV'],Matcho_outcome_limits)

    ab_episodes = define_outcome(visit_table,HIP_concepts,['SA','AB'],Matcho_outcome_limits)

    add_sb = add_outcome(lb_episodes,sb_episodes,Matcho_outcome_limits)

    add_ect = add_outcome(add_sb,ect_episodes,Matcho_outcome_limits)

    add_ab = add_outcome(add_ect,ab_episodes,Matcho_outcome_limits)

    add_deliv = add_outcome(add_ab,deliv_episodes,Matcho_outcome_limits)

    startdates = calc_startdate(add_deliv,Matcho_term_durations)

    gest_episodes = get_gest(visit_table,HIP_concepts)

    add_gest = add_gestation(startdates,gest_episodes)

    cleaned_eps = clean_episodes(add_gest)

    drop_overlaps = remove_overlaps(cleaned_eps)

    final_episodes_w_length = final_episodes(drop_overlaps,gest_episodes)
    
    return final_episodes_w_length

