#!/bin/bash

source env/bin/activate

esl db drop
esl db init

esl exposure add model/exposure_full.xml full_model
esl exposure add model/exposure.xml test_model

esl vulnerability add model/structural_vulnerability.xml struc_mmi
esl vulnerability add model/contents_vulnerability.xml cont_mmi

deactivate