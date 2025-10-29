
import pandas as pd
import numpy as np
import os

# Pull timing based concepts

def timing_pull(concepts_table):

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
            conc.concept_name,
            con.condition_concept_id as concept_id,
            NULL as value_as_number,
            conc.concept_name as value_as_string,
            'Condition' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.condition_occurrence con
            on vis.visit_occurrence_id = con.visit_occurrence_id
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.concept conc
            on con.condition_concept_id = conc.concept_id 
        where 
            vis.person_id in (select person_id from AFAB)
            and (
                con.condition_concept_id in """ + concepts + """
                or conc.concept_name like '%gestation period,%'
                )

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            conc.concept_name,
            obs.observation_concept_id as concept_id,
            value_as_number,
            value_as_string,
            'Observation' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.observation obs
            on vis.visit_occurrence_id = obs.visit_occurrence_id
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.concept conc
            on obs.observation_concept_id = conc.concept_id 
        where 
            vis.person_id in (select person_id from AFAB)
            and (
                obs.observation_concept_id in """ + concepts + """
                or conc.concept_name like '%gestation period,%'
                )

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            conc.concept_name,
            proc.procedure_concept_id as concept_id,
            NULL as value_as_number,
            conc.concept_name as value_as_string,
            'Procedure' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.procedure_occurrence proc
            on vis.visit_occurrence_id = proc.visit_occurrence_id
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.concept conc
            on proc.procedure_concept_id = conc.concept_id 
        where 
            vis.person_id in (select person_id from AFAB)
            and (
                proc.procedure_concept_id in """ + concepts + """
                or conc.concept_name like '%gestation period,%'
                )

        union distinct

        select distinct
            vis.person_id,
            vis.visit_end_date,
            conc.concept_name,
            mes.measurement_concept_id as concept_id,
            value_as_number,
            '' as value_as_string,
            'Measurement' as Concept_Domain
        from 
        """ + os.environ["WORKSPACE_CDR"] + """.visit_occurrence vis
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.measurement mes
            on vis.visit_occurrence_id = mes.visit_occurrence_id
        inner join 
        """ + os.environ["WORKSPACE_CDR"] + """.concept conc
            on mes.measurement_concept_id  = conc.concept_id 
        where 
            vis.person_id in (select person_id from AFAB)
            and (
                mes.measurement_concept_id in """ + concepts + """
                or conc.concept_name like '%gestation period,%'
                )



            """

    visit_table = pd.read_gbq(
        visit_query,
        dialect="standard",
        use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
        progress_bar_type="tqdm_notebook")
    
    return visit_table


def get_timing_info(HIP_PPS_merged,PPS_concept_file):

    PPS_concepts = pd.read_csv(PPS_concept_file)
    
    # Additional concepts related to timing
    observation_concept_list = [3011536, 3026070, 3024261, 4260747,40758410, 3002549, 43054890, 46234792, 4266763, 40485048, 3048230, 3002209, 3012266]
    measurement_concept_list = [3036844, 3048230, 3001105, 3002209, 3050433, 3012266]

    est_date_of_delivery_concepts = [1175623, 1175623, 3001105, 3011536, 3024261, 3024973, 3026070, 3036322, 3038318, 3038608, 4059478, 4128833, 40490322, 40760182, 40760183, 42537958]
    est_date_of_conception_concepts = [3002314, 3043737, 4058439, 4072438, 4089559, 44817092]
    len_of_gestation_at_birth_concepts = [4260747, 43054890, 46234792, 4266763, 40485048]

    timing_concepts = observation_concept_list + measurement_concept_list + est_date_of_delivery_concepts + est_date_of_conception_concepts + len_of_gestation_at_birth_concepts

    # Add concept IDs to PPS concepts
    timing_frame = (
        pd.concat(
            [
                PPS_concepts[['domain_concept_id']],
                pd.DataFrame(
                    {'domain_concept_id' : timing_concepts}
                )
            ]
        )
        .rename({'domain_concept_id' : 'concept_id'},axis=1)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    timing_visits = timing_pull(timing_frame)

    # Get timing concepts in each episode
    timing_match = (
        pd.merge(
            HIP_PPS_merged,
            timing_visits
            .assign(visit_end_date = lambda x : pd.to_datetime(x.visit_end_date)),
            how = 'left'
        )
        .query('visit_end_date.isnull()'
               'or merged_start <= visit_end_date and merged_end >= visit_end_date')
    )

    # Get extrapolated preg date from gw concept
    # Flag type of timing concepts on episode
    timing_joined = (
        pd.merge(
            pd.concat(
                [
                    timing_match,
                    pd.merge(
                        HIP_PPS_merged,
                        timing_match[['person_id','HIP_episode','PPS_episode']]
                        .drop_duplicates()
                        .assign(chk = True),
                        how = 'left'
                    )
                    .query('chk.isnull()')
                    .reset_index(drop=True)
                    .drop(['chk'],axis=1)
                ]
            )
            .sort_values(['person_id','merged_start'])
            .reset_index(drop=True)
            .assign(
                val = lambda x : x.value_as_string.apply(
                    lambda y : 
                    np.NaN 
                    if pd.isnull(y)
                    else (
                        int(y.lstrip('Gestation period, ').rstrip(' weeks'))
                        if y.lstrip('Gestation period, ').rstrip(' weeks').isnumeric()
                        else np.NaN
                    )
                ),
                value_as_number = lambda x : x.value_as_number.combine_first(x.val),
                extrapolated_preg_date = lambda x : 
                x.apply(
                    lambda y : 
                    y.visit_end_date - pd.to_timedelta(y.value_as_number*7,unit='days') 
                    if (~pd.isnull(y.concept_id) if pd.isnull(y.concept_id) else (y.concept_id in [3048230,3002209,3012266])) 
                    | (~pd.isnull(y.concept_name) if pd.isnull(y.concept_name) else ('Gestation period, ' in y.concept_name))
                    else pd.NaT,
                    axis=1
                )
            )
            .drop(['value_as_string','val'],axis=1),
            PPS_concepts
            .rename({'domain_concept_id' : 'concept_id'},axis=1),
            how='left'
        )
        .assign(
            GT_type_1 = lambda x : 
            x.concept_name.apply(
                lambda y : np.NaN if pd.isnull(y) else ('GW' if ('Gestation period, ' in y) else np.NaN)
            ),
            GT_type_2 = lambda x : 
            x.concept_id.apply(
                lambda y : np.NaN if pd.isnull(y) else ('GW' if (y in [3048230,3002209,3012266]) else np.NaN)
            ),
            GT_type_3 = lambda x : 
            x.min_month.apply(
                lambda y : np.NaN if pd.isnull(y) else ('GR3m')
            ),
            GT_type = lambda x : x.GT_type_1.combine_first(x.GT_type_2.combine_first(x.GT_type_3))
        )
        .drop(['GT_type_1','GT_type_2','GT_type_3'],axis=1)
    )

    # Get estimate of min and max start date from timing concept
    timing_info = (
        timing_joined
        .query("(GT_type == 'GW' and ~value_as_number.isnull()) or (GT_type == 'GR3m')")
        .assign(
            min_pregnancy_start = (
                lambda x : 
                x.apply(
                    lambda y : np.NaN if y.GT_type != 'GR3m' else y.visit_end_date - pd.to_timedelta(int(y.max_month*30.4),unit='days'),
                    axis = 1
                )
            ),
            max_pregnancy_start = (
                lambda x : 
                x.apply(
                    lambda y : np.NaN if y.GT_type != 'GR3m' else y.visit_end_date - pd.to_timedelta(int(y.min_month*30.4),unit='days'),
                    axis = 1
                )
            ),
            concept_name_binned = (
                lambda x : 
                x.apply(
                    lambda y : y.concept_name if y.GT_type == 'GR3m' else 'Gestation Week',
                    axis = 1
                )
            )
        )
        .sort_values(['person_id','episode','value_as_number'],ascending=[True,True,False])
        .drop_duplicates(['person_id','episode','concept_name_binned','visit_end_date','GT_type'])
        .sort_values(['person_id','episode','value_as_number'],ascending=[True,True,False])
    )
    
    return timing_info

# Helper function to get precision category from estimated date range
def get_precision_cat(x):
    return (
        x.precision_days.apply(
            lambda y : 'week_poor-support' if y == -1 else (
                'week' if y >= 0 and y <= 7 else (
                    'two-week' if y > 7 and y <= 14 else (
                        'three-week' if y > 14 and y <= 21 else (
                            'month' if y > 21 and y <= 28 else (
                                'two-month' if y > 28 and y <= 56 else (
                                    'three-month' if y > 56 and y <= 84 else 'non-specific'
                                )
                            )
                        )
                    )
                )
            )
        )
    )

def infer_start_dates(timing_info):

    # Count Gr3m concepts that give overlap in estimated start with other episodes
    overlap_counts = (
        pd.merge(
            timing_info[['person_id','episode','min_pregnancy_start','max_pregnancy_start','GT_type']]
            .query("GT_type == 'GR3m'")
            .drop_duplicates(),
            timing_info[['person_id','episode','min_pregnancy_start','max_pregnancy_start','GT_type']]
            .query("GT_type == 'GR3m'")
            .drop_duplicates()
            .rename({'min_pregnancy_start':'overlap_chk_min','max_pregnancy_start':'overlap_chk_max'},axis=1)
        )
        .query('((overlap_chk_min == min_pregnancy_start) or (overlap_chk_min == max_pregnancy_start)'
               ' or (overlap_chk_max == min_pregnancy_start) or (overlap_chk_max == max_pregnancy_start)'
               ' or ((overlap_chk_min >= min_pregnancy_start) and (overlap_chk_min <= max_pregnancy_start))'
               ' or ((overlap_chk_max >= min_pregnancy_start) and (overlap_chk_max <= max_pregnancy_start)))'
               #' and ~((overlap_chk_min == min_pregnancy_start) and (overlap_chk_max == max_pregnancy_start))'
              )
        .assign(overlap_count = 1)
        .groupby(['person_id','episode','min_pregnancy_start','max_pregnancy_start','GT_type'])['overlap_count']
        .sum()
        .reset_index()
        .assign(overlap_count = lambda x : x.overlap_count - 1)
    )

    # Find Q1 and Q3 of overlapping concepts 
    Q1 = (
        overlap_counts
        .groupby(['person_id','episode'])['overlap_count']
        .quantile(.25)
    )

    Q3 = (
        overlap_counts
        .groupby(['person_id','episode'])['overlap_count']
        .quantile(.75)
    )

    # Set threshold for concept to be considered in interval
    threshold = (
        np.abs(Q1 - 1.5 * (Q3 - Q1))
    ).reset_index().rename({'overlap_count' : 'threshold'},axis=1)

    # Filter out concepts with too few overlaps
    # Get intersection of implied start ranges
    # Remove min and max starts not in intersection
    # Find min and max start in intersection
    # Get length and midpoint of intersection, increase length to minimum of 7 days
    intersections = (
        pd.merge(
            overlap_counts,
            threshold
        )
        .query('(overlap_count >= threshold)')
        .assign( 
            min_min = lambda x : x.groupby(['person_id','episode'])[['min_pregnancy_start']].transform(min),
            max_max = lambda x : x.groupby(['person_id','episode'])[['max_pregnancy_start']].transform(max),
            max_min = lambda x : x.groupby(['person_id','episode'])[['min_pregnancy_start']].transform(max),
            min_max = lambda x : x.groupby(['person_id','episode'])[['max_pregnancy_start']].transform(min),
            min_pregnancy_start2 = lambda x : x.apply(lambda y : y.min_pregnancy_start if y.min_pregnancy_start <= y.min_max else np.NaN,axis=1),
            max_pregnancy_start2 = lambda x : x.apply(lambda y : y.max_pregnancy_start if y.max_pregnancy_start >= y.max_min else np.NaN,axis=1),
            min_int_start = lambda x : (
                x
                .groupby(['person_id','episode'])[['min_pregnancy_start2']].transform(max)
            ),
            max_int_start = lambda x : (
                x
                .groupby(['person_id','episode'])[['max_pregnancy_start2']].transform(min)
            )
        )
        .drop(['min_pregnancy_start','max_pregnancy_start','GT_type','overlap_count','threshold','max_min','min_max','min_pregnancy_start2','max_pregnancy_start2'],axis=1)
        .drop_duplicates()
        .assign(
            max_range_days = lambda x : (x.max_max - x.min_min).dt.days,
            plausibledays = lambda x : (x.max_int_start - x.min_int_start).dt.days,
            midpoint = lambda x : x.min_int_start + pd.to_timedelta(round(x.plausibledays/2),unit='days'),
            min_int_start = lambda x : (
                x.apply( lambda y :
                        y.min_int_start if y.plausibledays >= 7 else y.midpoint - pd.to_timedelta(3,unit='days'),
                        axis = 1
                       )
            ),
            max_int_start = lambda x : (
                x.apply( lambda y :
                        y.max_int_start if y.plausibledays >= 7 else y.midpoint + pd.to_timedelta(3,unit='days'),
                        axis = 1
                       )
            ),
            plausible_days = lambda x : (
                x.apply( lambda y :
                        y.plausibledays if y.plausibledays >= 7 else (x.max_int_start - x.min_int_start).dt.days,
                        axis = 1
                       )
            )
        )
        .drop(['plausibledays'],axis=1)
    )

    # Find GW events with expected start in intersection
    # Get proportion that fall in intersection
    get_overlap_GW = (
        pd.merge(
            timing_info
            .query("GT_type == 'GW'")[['person_id','episode','extrapolated_preg_date']],
            intersections,
            how='outer'
        )
        .assign(
            overlap = lambda x : (x.extrapolated_preg_date >= x.min_int_start) & (x.extrapolated_preg_date <= x.max_int_start),
            overlapping = lambda x : (
                x
                .groupby(['person_id','episode'])['overlap']
                .transform('mean')
            )
        )
    )

    # Find distance of extrapolated pregnancy start from median
    # Only us GW events in intersection when greater than 50% are in intersection 
    get_dist = (
        get_overlap_GW
        .query('(overlap == True and overlapping > .5) or (overlapping <= .5 and ~extrapolated_preg_date.isnull())')[['person_id','episode','extrapolated_preg_date']]
        .assign( med = lambda x :
                x.groupby(['person_id','episode'])['extrapolated_preg_date'].transform('median'),
                dist = lambda x : np.abs((x.extrapolated_preg_date -x.med) / pd.to_timedelta(1,unit='days'))
               )

    )

    # Get Q1, Q3 and M of GW distances, set thresholds for events to use
    Q1 = (
        get_dist
        .groupby(['person_id','episode'])['dist']
        .quantile(.25)
    )

    M = (
        get_dist
        .groupby(['person_id','episode'])['dist']
        .quantile(.5)
    )

    Q3 = (
        get_dist
        .groupby(['person_id','episode'])['dist']
        .quantile(.75)
    )

    l_threshold = (
        M - 1.5 * (Q3 - Q1)
    ).reset_index().rename({'dist' : 'lower_out'},axis=1)


    u_threshold = (
        M + 1.5 * (Q3 - Q1)
    ).reset_index().rename({'dist' : 'upper_out'},axis=1)

    # Using GW events within threshold, count GW dates close to middle of range
    inferred_GW = (
        pd.merge(
            get_dist,
            pd.merge(
                l_threshold,
                u_threshold
            )
        )
        .query('(dist >= lower_out) & (dist <= upper_out)')
        .assign(
            inferred_start_date = lambda x : 
            x.groupby(['person_id','episode'])['extrapolated_preg_date'].transform('first'),
            date_count = lambda x : 
            x.groupby(['person_id','episode'])['extrapolated_preg_date'].transform('count').apply(lambda y : np.NaN if y > 1 else -1),
            precision_days = lambda x : 
            x.date_count.combine_first(
                (x.groupby(['person_id','episode'])['extrapolated_preg_date'].transform('max') -
                 x.groupby(['person_id','episode'])['extrapolated_preg_date'].transform('min')).dt.days
            ).astype(int)
        )
        .drop(['med','dist','lower_out','upper_out','extrapolated_preg_date','date_count'],axis=1)
        .drop_duplicates()
    )

    # Take midpoint of range as inferred start
    # Get max of range as precise range
    # Assign precision category
    timing_final = (
        pd.concat(
            [
                inferred_GW,
                get_overlap_GW
                .query('extrapolated_preg_date.isnull()')
                .assign(
                    inferred_start_date = lambda x : x.midpoint,
                    precision_days = lambda x : x.max_range_days.astype(int)
                )[['person_id','episode','inferred_start_date','precision_days']]
            ]
        )
        .reset_index(drop=True)
        .assign(
            precision_category = lambda x : get_precision_cat(x)

        )
    )
    
    return timing_final

# Get fianl outcome determination for episodes and label episode numbers
def get_final_outcomes(HIP_PPS_merged,timing_final,term_durations_file):

    final_outcomes = (
        pd.merge(
            timing_final,
            HIP_PPS_merged,
            how='right'
        )
        .assign(
            outcome_match = lambda x : (
                x.apply( lambda y : 
                    y.HIP_category == y.PPS_category and (np.abs((y.HIP_end- y.PPS_outcome_date).days) <= 14)
                    ,axis=1
                )
            ),
            final_outcome_category = lambda x : (
                x.apply( lambda y : 
                    y.HIP_category if y.outcome_match or pd.isnull(y.PPS_category)
                        else (y.PPS_category if pd.isnull(y.HIP_category) 
                              else y.PPS_category if (y.HIP_category != 'PREG' and (y.HIP_end <= y.PPS_outcome_date - pd.to_timedelta(7,unit='days'))) 
                                    else y.HIP_category)
                    ,axis=1
                ).fillna('PREG')
            ),
            inferred_end_date = lambda x : (
                x.apply( lambda y : 
                    y.HIP_end if y.outcome_match or pd.isnull(y.PPS_category)
                        else (y.PPS_outcome_date if pd.isnull(y.HIP_category) 
                              else y.PPS_outcome_date if (y.HIP_category != 'PREG' and (y.HIP_end <= y.PPS_outcome_date - pd.to_timedelta(7,unit='days'))) 
                                    else y.HIP_end)
                    ,axis=1
                ).combine_first(x.merged_end)
            )
        )
    )

    final_episodes = (
        pd.merge(
            final_outcomes,
            pd.read_csv(term_durations_file)
            .drop(['retry'],axis=1)
            .rename({'category' : 'final_outcome_category'},axis=1),
            how='left'
        )
        .assign(
            inferred_start_date = lambda x : x.inferred_start_date.combine_first(x.inferred_end_date - pd.to_timedelta(x.max_term,unit='days')),
            precision_days = lambda x : x.precision_days.combine_first((x.inferred_end_date - x.inferred_start_date).dt.days),
            precision_category = lambda x : x.precision_category.combine_first(get_precision_cat(x)),
            gestational_age = lambda x : (x.inferred_end_date - x.inferred_start_date).dt.days
        )
        .query('~inferred_start_date.isnull()')
        .assign(
            episode = lambda x : x.groupby(['person_id'])['episode'].transform('rank').astype(int)
        )
    )
    
    return final_episodes

def main(HIP_PPS_merged,PPS_concept_file,term_durations_file):
    
    timing_info = get_timing_info(HIP_PPS_merged,PPS_concept_file)
    
    timing_final = infer_start_dates(timing_info)
    
    final_episodes = get_final_outcomes(HIP_PPS_merged,timing_final,term_durations_file)
            
    return final_episodes
