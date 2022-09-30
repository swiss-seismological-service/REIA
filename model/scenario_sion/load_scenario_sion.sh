#!/bin/bash

source ../../env/bin/activate

# esl db drop
# esl db init

esl exposure add exposure/Exposure_1km_v03a_CH_modalAmpl_amplAvg.xml scenario_sion

# esl vulnerability add vulnerability/content_vulnerability_model_V003adjusted_BT.xml scenario_sion_content
# esl vulnerability add vulnerability/displaced_ST_vulnerability_model_V003adjusted_mean.xml scenario_sion_displaced
# esl vulnerability add vulnerability/fatality_vulnerability_model_V003adjusted_mean.xml scenario_sion_fatality
# esl vulnerability add vulnerability/injury_vulnerability_model_V003adjusted_mean.xml scenario_sion_injury
esl vulnerability add vulnerability/structural_vulnerability_model_V003adjusted_BT.xml scenario_sion_structural

deactivate