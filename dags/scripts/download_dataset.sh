#!/bin/bash

path_to_local_home="$1"
dataset_source="$2"

# kaggle datasets download -d olistbr/brazilian-ecommerce -f $dataset_source -p $path_to_local_home --unzip
curl -sfSL https://raw.githubusercontent.com/MartinRodriguez93/itba_final_project/main/source_system/${dataset_source} > $path_to_local_home/$dataset_source