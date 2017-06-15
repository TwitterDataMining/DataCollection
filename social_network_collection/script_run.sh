#!/usr/bin/env bash
declare -a tvshows=("Powerless" "24Legacy" "LegionFX")

for show in "${tvshows[@]}"
do
  echo "updating "$show" "
  python update_user_ids.py $show
done
