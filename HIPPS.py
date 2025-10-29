
import HIP
import PPS
import HIP_PPS_Merge
import ESD
import utilities

def main(HIP_concept_file,outcome_limits_file, term_durations_file, PPS_concept_file):

    HIP_eps, HIP_trimester_events, HIP_trimester_summary, HIP_trimester_event_details = HIP.main(HIP_concept_file,
                                                                                               outcome_limits_file,
                                                                                               term_durations_file
                                                                                              )

    PPS_eps, PPS_trimester_events, PPS_trimester_summary, PPS_trimester_event_details = PPS.main(PPS_concept_file
                                                                                                )

    HIP_PPS_merged = HIP_PPS_Merge.main(HIP_eps,
                                       PPS_eps,
                                       HIP_concept_file,
                                       PPS_concept_file
                                      )

    final_episodes = ESD.main(HIP_PPS_merged,
                              PPS_concept_file,
                              term_durations_file
                             )
        
    final_episodes = utilities.calculate_concordance_score(final_episodes)
    
    
    return final_episodes
