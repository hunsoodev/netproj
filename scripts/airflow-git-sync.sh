#!/bin/bash

cd ~/netproj

current_branch=$(git branch --show-current)
flag=false

if ["$current_branch" != "main"]; then
	echo "Not on main branch. Executing git stash..."
	git stash
	echo "Switching to 'main' branch..."
	git switch main
	flag=true
else
	echo "On main branch. No action needed."
fi

git fetch -all

if git diff main origin-https/main --name-only | grep -q 'docker-compose.yaml'; then
	docker compose down && docker compose up -d 
else
	echo "No changes in 'docker-compose.yaml'."
fi

git pull

if ["$flag" = true]; then
	git switch $current_branch
	git stach pop
fi



