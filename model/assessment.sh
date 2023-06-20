#!/bin/bash

source env/bin/activate
mkdir -p c21pOmNoLmV0aHouc2VkL3NjMjBkL09yaWdpbi9OTEwuMjAyMzA1MjkxOTE3NDYuNDM2ODE5LjMwOTky
cd c21pOmNoLmV0aHouc2VkL3NjMjBkL09yaWdpbi9OTEwuMjAyMzA1MjkxOTE3NDYuNDM2ODE5LjMwOTky
reia gmfs sample ../test_model/c21pOmNoLmV0aHouc2VkL3NjMjBkL09yaWdpbi9OTEwuMjAyMzA1MjkxOTE3NDYuNDM2ODE5LjMwOTky/current/products/
reia risk-assessment run smi:ch.ethz.sed/sc20d/Origin/NLL.20230529191746.436819.30992 --loss ../model/risk.ini --damage ../model/damage.ini