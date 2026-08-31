#!/bin/bash
# Finds scripts referenced by spicer_ops_menu.py that don't actually exist on disk
cd /home/rellis/spicer
MISSING=0
while IFS= read -r name; do
  [[ -z "" ]] && continue
  if [[ ! -f "scripts/" ]]; then
    echo "MISSING: scripts/"
    MISSING=1
  fi
done < <(grep -h 'SCRIPTS_DIR /' spicer_ops_menu.py | sed 's/.*SCRIPTS_DIR \/ //;s/[^a-z_.]*//g' | sort -u)
if [[ $MISSING -eq 0 ]]; then
  echo "All referenced scripts are present."
fi
