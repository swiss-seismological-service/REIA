#!/bin/bash

source ../../env/bin/activate

esl db drop
esl db init

esl exposure add exposure.xml test_model

esl vulnerability add structural_vulnerability.xml struc_mmi
esl vulnerability add contents_vulnerability.xml cont_mmi

deactivate