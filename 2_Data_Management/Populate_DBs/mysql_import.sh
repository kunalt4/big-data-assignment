#!/bin/bash

if [ $# -lt 2 ]; then
	echo "Two arguments needed:"
	echo "1. mysql username"
	echo "2. mysql password"
	exit 1
fi

mysql -u "$1" -p"$2" -e "DROP DATABASE IF EXISTS players_teams_positions; CREATE DATABASE players_teams_positions;"

input="Teams.csv"
string="INSERT into Team_details values"
count=1
{
	read continue
	while IFS= read -r line || [ -n "$line" ]; do
		A="$(echo $line | cut -d',' -f1)"
		B="$(echo $line | cut -d',' -f2)"
		C="$(echo $line | cut -d',' -f3)"
		D="$(echo $line | cut -d',' -f4)"
		E="$(echo $line | cut -d',' -f5)"
		F="$(echo $line | cut -d',' -f6)"
		G="$(echo $line | cut -d',' -f7)"
		H="$(echo $line | cut -d',' -f8)"
		I="$(echo $line | cut -d',' -f9)"
		J="$(echo $line | cut -d',' -f10)"
		string="$string($count,\"${A}\",${B},${C},${D},${E},${F},${G},${H},${I},${J}),"
		count=$((count + 1))
	done
} <$input
string="${string%?}"
string="$string;"

mysql -u "$1" -p"$2" -e "use players_teams_positions; CREATE TABLE Team_details(t_id int not null primary key, team_name char(30) not null, ranking int not null, games int not null, wins int not null, draws int not null, losses int not null, goals_for int not null, goals_against int not null, yellow_cards int not null, red_cards int not null); $string"

cut -d, -f3 Players.csv | sort -u | head -n -1 >positions.csv

input="positions.csv"
string="INSERT INTO Positions(pos_name) values"
{
	while IFS= read -r line || [ -n "$line" ]; do
		A="$(echo $line | cut -d',' -f1)"
		string="$string(\"${A}\"),"
	done
} <$input
string="${string%?}"
string="$string;"

mysql -u "$1" -p"$2" -e "use players_teams_positions; CREATE TABLE Positions (pos_id int not null auto_increment primary key, pos_name char(30) not null); $string"

input="Players.csv"
string="INSERT into Player_details values"
count=1
{
	read continue
	while IFS= read -r line || [ -n "$line" ]; do
		A="$(echo $line | cut -d',' -f1)"
		B="$(echo $line | cut -d',' -f2)"
		C="$(echo $line | cut -d',' -f3)"
		D="$(echo $line | cut -d',' -f4)"
		E="$(echo $line | cut -d',' -f5)"
		F="$(echo $line | cut -d',' -f6)"
		G="$(echo $line | cut -d',' -f7)"
		H="$(echo $line | cut -d',' -f8)"
		string="$string($count,\"${A}\",(SELECT t_id from Team_details where team_name = \"${B}\"),(SELECT pos_id from Positions where pos_name =\"${C}\"),${D},${E},${F},${G},${H}),"
		count=$((count + 1))
	done
} <$input
string="${string%?}"
string="$string;"

mysql -u "$1" -p"$2" -e "use players_teams_positions; CREATE TABLE Player_details(p_id int not null primary key, name char(30) not null, t_id int not null, pos_id int not null, minutes int not null, shots int not null, passes int not null, tackles int not null, saves int not null, foreign key (t_id) references Team_details(t_id), foreign key (pos_id) references Positions(pos_id));; $string"
