#!/bin/sh

./bin/importer --file ../../../Downloads/housing.csv | 
./bin/dump 2> ./tmp/initial.log | 
./bin/project --cols "median_house_value,total_rooms,ocean_proximity,median_income,households,housing_median_age" |
./bin/transform --statement "map(housing_median_age,int(replace(#,'.0','')))" |
./bin/transform --statement "map(total_rooms,int(replace(#,'.0','')))" |
./bin/transform --statement "map(households,int(replace(#,'.0','')))" |
./bin/transform --statement "map(median_income,int(replace(#,'.','')))" |
./bin/transform --statement "map(median_house_value,int(replace(#,'.','')))" |
./bin/where -cond "housing_median_age >= 42" | 
./bin/where -cond "ocean_proximity=='NEAR BAY'" | 
./bin/transform --statement "let total_households=reduce(households, #acc + #,0);total_households" |
./bin/transform --statement "let num_recs=len(households);num_recs" |
./bin/dump 2> ./tmp/final.log 1> ./tmp/result.gob