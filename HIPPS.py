#!/usr/bin/env python
# coding: utf-8

# In[1]:


import HIP
import PPS
import HIP_PPS_Merge
import ESD

def main(HIP_concept_file,outcome_limits_file, term_durations_file, PPS_concept_file):

    HIP_eps = HIP.main(HIP_concept_file,
                   outcome_limits_file,
                   term_durations_file
                  )

    PPS_eps = PPS.main(PPS_concept_file
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
    
    return final_episodes

