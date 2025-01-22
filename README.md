# pyspark_optimization
I have taken a big csv file from kaggle for this project, It's size is around 750 MB. It gives error when I try to load the data in pandas. I have performed certain step using pyspark and it takes 45seconds to process the data. It is a marathon dataset.

### Physical execution plan before Optimization

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [athlete_age_category#197, athlete_gender#183, event_year#56, event_dates#71, event_name#85, event_distance#99, event_num_finishers#253, athlete_performance#127, athlete_club#141, athlete_country#155, athlete_birth_year#169, athlete_avg_speed#239, athlete_id#225, athlete_age_at_event#267, event_distance_km#282, rank_in_event#300, avg_speed_category#340]
   +- SortMergeJoin [athlete_age_category#197, athlete_gender#183], [athlete_age_category#357, athlete_gender#359], Inner
      :- Sort [athlete_age_category#197 ASC NULLS FIRST, athlete_gender#183 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(athlete_age_category#197, athlete_gender#183, 200), ENSURE_REQUIREMENTS, [plan_id=316]
      :     +- Filter (isnotnull(athlete_age_category#197) AND isnotnull(athlete_gender#183))
      :        +- Window [rank(athlete_avg_speed#239) windowspecdefinition(event_name#85, event_year#56, athlete_avg_speed#239 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_in_event#300], [event_name#85, event_year#56], [athlete_avg_speed#239 DESC NULLS LAST]
      :           +- Sort [event_name#85 ASC NULLS FIRST, event_year#56 ASC NULLS FIRST, athlete_avg_speed#239 DESC NULLS LAST], false, 0
      :              +- Exchange hashpartitioning(event_name#85, event_year#56, 200), ENSURE_REQUIREMENTS, [plan_id=308]
      :                 +- Project [Year of event#17 AS event_year#56, Event dates#18 AS event_dates#71, Event name#19 AS event_name#85, Event distance/length#20 AS event_distance#99, Event number of finishers#21 AS event_num_finishers#253, Athlete performance#22 AS athlete_performance#127, Athlete club#23 AS athlete_club#141, Athlete country#24 AS athlete_country#155, Athlete year of birth#25 AS athlete_birth_year#169, Athlete gender#26 AS athlete_gender#183, Athlete age category#27 AS athlete_age_category#197, cast(Athlete average speed#28 as float) AS athlete_avg_speed#239, Athlete ID#29 AS athlete_id#225, (cast(Year of event#17 as double) - Athlete year of birth#25) AS athlete_age_at_event#267, cast(regexp_extract(Event distance/length#20, (\d+\.?\d*), 1) as float) AS event_distance_km#282]
      :                    +- FileScan csv [Year of event#17,Event dates#18,Event name#19,Event distance/length#20,Event number of finishers#21,Athlete performance#22,Athlete club#23,Athlete country#24,Athlete year of birth#25,Athlete gender#26,Athlete age category#27,Athlete average speed#28,Athlete ID#29] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year of event:int,Event dates:string,Event name:string,Event distance/length:string,Event ...
      +- Sort [athlete_age_category#357 ASC NULLS FIRST, athlete_gender#359 ASC NULLS FIRST], false, 0
         +- HashAggregate(keys=[athlete_age_category#357, athlete_gender#359], functions=[avg(athlete_avg_speed#239)])
            +- Exchange hashpartitioning(athlete_age_category#357, athlete_gender#359, 200), ENSURE_REQUIREMENTS, [plan_id=312]
               +- HashAggregate(keys=[athlete_age_category#357, athlete_gender#359], functions=[partial_avg(athlete_avg_speed#239)])
                  +- Project [Athlete gender#353 AS athlete_gender#359, Athlete age category#354 AS athlete_age_category#357, cast(Athlete average speed#355 as float) AS athlete_avg_speed#239]
                     +- Filter (isnotnull(Athlete age category#354) AND isnotnull(Athlete gender#353))
                        +- FileScan csv [Athlete gender#353,Athlete age category#354,Athlete average speed#355] Batched: false, DataFilters: [isnotnull(Athlete age category#354), isnotnull(Athlete gender#353)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv], PartitionFilters: [], PushedFilters: [IsNotNull(Athlete age category), IsNotNull(Athlete gender)], ReadSchema: struct<Athlete gender:string,Athlete age category:string,Athlete average speed:string>


