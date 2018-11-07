#!/usr/bin/env bash

# The following are commands used in the first class

conda create --name MSDS694 python=3.6 -y

#
# To activate this environment, use:
# > source activate MSDS694
#
# To deactivate an active environment, use:
# > source deactivate
#


conda info --envs # this gives us info about base, current etc.

conda env export # shows what is in our environment (e.g. to check if numpy is there)

conda env export > MSDS_environment.yml # exports output to a YML file

conda remove --name MSDS694 --all # remove

conda env create -f MSDS694_environment.yml -n MSDS694 # use existing yml file to create a brand new
# environment that is called MSDS694

# note: we installed pip install pycodestyle

# note: the hammer thing in jupyter notebook


# Dot this part: file:///home/louiselai88gmail/Desktop/School/Distributed%20Computing/2018_DistributedComputing_Week1_v061.pdf