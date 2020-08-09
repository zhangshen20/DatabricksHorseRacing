-- Databricks notebook source
describe delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerPreviousStartsUnique`

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC 
-- MAGIC rm -R /dbfs/mnt/gamble/DELTA/SILVER/DATA/RunnerPreviousStartsUnique

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC rm -R /dbfs/mnt/gamble/DELTA/SILVER/CHECKPOINT/RunnerPreviousStartsUnique

-- COMMAND ----------

Create OR Replace view HorseRacing.view_OneRaceRunner AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/OneRaceRunner`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_Meetings AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/Meetings`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_OneRace AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/OneRace`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_OneRacePredictions AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/OneRacePredictions`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_OneRaceRatings AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/OneRaceRatings`

-- COMMAND ----------

use HorseRacing
;
-- select * from view_OneRaceRunner limit 100
-- ;
-- select raceNumber, * from view_Meetings limit 100
-- ;
-- select raceNumber, * from view_OneRace limit 100
-- ;
-- select *
-- from view_Meetings L 
-- --       LEFT OUTER JOIN view_OneRace as R
--       INNER JOIN view_OneRace R
--         on L.meetingName = R.meetingName and L.meetingDate = R.meetingDate and L.raceType = R.raceType and L.raceNumber = R.raceNumber
-- limit 100      

-- select * from view_OneRacePredictions limit 100
-- select * from view_OneRaceRatings limit 100
-- ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC DFTemp = spark.sql("select * from   view_OneRaceRatings ")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC (
-- MAGIC spark.sql("select * from view_OneRacePredictions")
-- MAGIC   .select("raceNumber", "raceName", "meetingName", "meetingDate", col("predictions_betType").alias("predictions_betTypeRaw"), "predictions_runners_runnerNumber", "predictions_runners_probability")
-- MAGIC   .withColumn("predictions_betType", concat(lit("prediction_"), col("predictions_betTypeRaw"), lit("_probability")))
-- MAGIC   .groupby("raceNumber", "raceName", "meetingName", "meetingDate", "predictions_runners_runnerNumber", )
-- MAGIC   .pivot("predictions_betType")
-- MAGIC   .agg(min("predictions_runners_probability"))
-- MAGIC   .createOrReplaceTempView("view_OneRacePredictionsTemp")
-- MAGIC )
-- MAGIC   

-- COMMAND ----------

select * from view_OneRacePredictionsTemp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC (
-- MAGIC spark.sql("select * from view_OneRaceRatings ")
-- MAGIC     .select("raceNumber", "raceName", "meetingName", "meetingDate", concat(lit("rating_"), col("ratingType")).alias("ratingType"), posexplode("ratingRunnerNumbers").alias("ratingsRaw", "runnerNumber"))
-- MAGIC     .withColumn("ratings", col("ratingsRaw")+1)
-- MAGIC     .groupBy("raceNumber", "raceName", "meetingName", "meetingDate", "runnerNumber")
-- MAGIC     .pivot("ratingType")
-- MAGIC     .agg(min("ratings"))
-- MAGIC     .createOrReplaceTempView("view_OneRaceRatingsTemp")
-- MAGIC )

-- COMMAND ----------

select *
from view_OneRaceRatingsTemp


-- COMMAND ----------

Create OR Replace view HorseRacing.view_OneRaceResultRunner AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/OneRaceResultRunner`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_RunnerStarts AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerStarts`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_RunnerPreviousStartsUnique AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerPreviousStartsUnique`

-- COMMAND ----------

select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerPreviousStartsUnique`

-- COMMAND ----------

Create OR Replace view HorseRacing.view_RunnerWinningDistance AS select * from delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerWinningDistance`

-- COMMAND ----------

select * from HorseRacing.view_RunnerWinningDistance limit 100

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC (
-- MAGIC spark.sql("select * from view_RunnerWinningDistance ")
-- MAGIC     .select("meetingDate", "runnerNumber", "runnerName", "winningDistance", "numberOfWinsAtDistance")
-- MAGIC #     .withColumn("ratings", col("ratingsRaw")+1)
-- MAGIC     .groupBy("meetingDate", "runnerNumber", "runnerName")
-- MAGIC     .pivot("winningDistance")
-- MAGIC     .agg(min("numberOfWinsAtDistance"))
-- MAGIC     .createOrReplaceTempView("view_RunnerWinningDistanceTemp")
-- MAGIC )

-- COMMAND ----------

select * from view_RunnerWinningDistanceTemp limit 100

-- COMMAND ----------

Create OR Replace temporary view view_RunnerMasterTemp AS

select L.allIn, L.allowBundle, R.allowFixedOddsPlace, R.allowMulti, R.allowParimutuelPlace, R.apprenticesCanClaim, R.audio, 
--        R.betTypes, 
       R.broadcastChannel, R.broadcastChannels,
       R.cashOutEligibility, R.fixedOddsOnlineBetting, R.fixedOddsUpdateTime, R.hasEarlySpeedRatings, R.hasFixedOdds, R.hasForm, R.hasParimutuel, R.location, R.meetingDate,
       R.meetingName, R.multiLegApproximates, R.numberOfFixedOddsPlaces, R.numberOfPlaces, R.oddsUpdateTime, R.parimutuelPlaceStatus, 
--        R.pools, R.predictions,
       R.previewVideo, R.prizeMoney, R.raceClassConditions, R.raceDistance, R.raceName, R.raceNumber, R.raceStartTime, R.raceStatus, R.raceType, L.railPosition,
--        R.ratings, R.results, R.runners, L.scratchings, 
       R.sellCode_meetingCode, R.sellCode_scheduledType, L.skyRacing_audio, L.skyRacing_video, R.substitute, R.tipRunnerNumbers,
       R.tipster, R.tipType, L.trackCondition, R.trackDirection, R.venueMnemonic, L.weatherCondition, R.willHaveFixedOdds, R2.barrierNumber, R2.claimAmount, R2.dfsFormRating, 
       R2.earlySpeedRating, R2.earlySpeedRatingBand, R2.emergency, R2.fixedOdds_allowPlace, R2.fixedOdds_bettingStatus, 
--        R2.fixedOdds_differential, R2.fixedOdds_flucs, 
       R2.fixedOdds_isFavouritePlace,
       R2.fixedOdds_isFavouriteWin, R2.fixedOdds_percentageChange, R2.fixedOdds_propositionNumber, R2.fixedOdds_returnPlace, R2.fixedOdds_returnWin, R2.fixedOdds_returnWinOpen, 
       R2.fixedOdds_returnWinOpenDaily, R2.fixedOdds_returnWinTime, R2.handicapWeight, R2.harnessHandicap, R2.last5Starts, R2.parimutuel_bettingStatus, R2.parimutuel_isFavouriteExact2, 
       R2.parimutuel_isFavouritePlace, R2.parimutuel_isFavouriteWin, 
--        R2.parimutuel_marketMovers, 
       R2.parimutuel_percentageChange, R2.parimutuel_returnExact2, R2.parimutuel_returnPlace, 
       R2.parimutuel_returnWin, R2.penalty, R2.riderDriverFullName, R2.riderDriverName, R2.runnerName, R2.runnerNumber, R2.silkURL, R2.tcdwIndicators, R2.techFormRating,
       R2.totalRatingPoints, R2.trainerFullName, R2.trainerName, R2.vacantBox, R3.prediction_Place_probability, R3.prediction_Win_probability,
       R4.rating_Class, R4.rating_Distance, R4.rating_Last12Months, R4.rating_Overall, R4.rating_Rating, R4.rating_Recent, R4.rating_Time,
       R5.finishingPosition, R5.fixedOdds_placeDeduction, R5.fixedOdds_returnPlace as fixedOdds_returnPlace_close, R5.fixedOdds_returnWin as fixedOdds_returnWin_close,
       R5.fixedOdds_scratchedTime, R5.fixedOdds_winDeduction, R5.parimutuel_returnPlace as parimutuel_returnPlace_close, R5.parimutuel_returnWin as parimutuel_returnWin_close,
       R5.resultedTime, R5.results, R5.winBook, R6.age, R6.blinkers, R6.classLevel, R6.colour, R6.dam, R6.daysSinceLastRun, R6.fieldStrength, R6.formComment, 
--        R6.formComments,
       R6.last20Starts, R6.riderDriverStarts_last12Months_numberOfPlacings, R6.riderDriverStarts_last12Months_numberOfStarts, R6.riderDriverStarts_last12Months_numberOfWins,
       R6.riderDriverStarts_last30Days_numberOfPlacings, R6.riderDriverStarts_last30Days_numberOfStarts, R6.riderDriverStarts_last30Days_numberOfWins, R6.riderDriverStarts_region_numberOfPlacings,
       R6.riderDriverStarts_region_numberOfStarts, R6.riderDriverStarts_region_numberOfWins, R6.riderDriverStarts_runner_numberOfPlacings, R6.riderDriverStarts_runner_numberOfStarts,
       R6.riderDriverStarts_runner_numberOfWins, R6.riderDriverStarts_track_numberOfPlacings, R6.riderDriverStarts_track_numberOfStarts, R6.riderDriverStarts_track_numberOfWins,
       R6.riderOrDriver, R6.riderOrDriverSex, R6.runnerStarts_classSame_numberOfPlacings, R6.runnerStarts_classSame_numberOfStarts, R6.runnerStarts_classSame_numberOfWins, 
       R6.runnerStarts_classStronger_numberOfPlacings, R6.runnerStarts_classStronger_numberOfStarts, R6.runnerStarts_classStronger_numberOfWins, R6.runnerStarts_dead_numberOfPlacings, 
       R6.runnerStarts_dead_numberOfStarts, R6.runnerStarts_dead_numberOfWins, R6.runnerStarts_distance_numberOfPlacings, R6.runnerStarts_distance_numberOfStarts, R6.runnerStarts_distance_numberOfWins, 
       R6.runnerStarts_firm_numberOfPlacings, R6.runnerStarts_firm_numberOfStarts, R6.runnerStarts_firm_numberOfWins, R6.runnerStarts_firstUp_numberOfPlacings, R6.runnerStarts_firstUp_numberOfStarts,
       R6.runnerStarts_firstUp_numberOfWins, R6.runnerStarts_good_numberOfPlacings, R6.runnerStarts_good_numberOfStarts, R6.runnerStarts_good_numberOfWins, R6.runnerStarts_heavy_numberOfPlacings, 
       R6.runnerStarts_heavy_numberOfStarts, R6.runnerStarts_heavy_numberOfWins, R6.runnerStarts_overall_numberOfPlacings, R6.runnerStarts_overall_numberOfStarts, R6.runnerStarts_overall_numberOfWins, 
       R6.runnerStarts_secondUp_numberOfPlacings, R6.runnerStarts_secondUp_numberOfStarts, R6.runnerStarts_secondUp_numberOfWins, R6.runnerStarts_slow_numberOfPlacings, R6.runnerStarts_slow_numberOfStarts, 
       R6.runnerStarts_slow_numberOfWins, R6.runnerStarts_soft_numberOfPlacings, R6.runnerStarts_soft_numberOfStarts, R6.runnerStarts_soft_numberOfWins, R6.runnerStarts_track_numberOfPlacings, 
       R6.runnerStarts_track_numberOfStarts, R6.runnerStarts_track_numberOfWins, R6.runnerStarts_trackDistance_numberOfPlacings, R6.runnerStarts_trackDistance_numberOfStarts, 
       R6.runnerStarts_trackDistance_numberOfWins, R6.runsSinceSpell, R6.sex, R6.sire, R6.trainerLocation, 
--        R6.trainerName, 
       R6.trainerStarts_jockey_numberOfPlacings, R6.trainerStarts_jockey_numberOfStarts, 
       R6.trainerStarts_jockey_numberOfWins, R6.trainerStarts_last12Months_numberOfPlacings, R6.trainerStarts_last12Months_numberOfStarts, R6.trainerStarts_last12Months_numberOfWins,
       R6.trainerStarts_last30Days_numberOfPlacings, R6.trainerStarts_last30Days_numberOfStarts, R6.trainerStarts_last30Days_numberOfWins, R6.trainerStarts_region_numberOfPlacings,
       R6.trainerStarts_region_numberOfStarts, R6.trainerStarts_region_numberOfWins, R6.trainerStarts_track_numberOfPlacings, R6.trainerStarts_track_numberOfStarts, R6.trainerStarts_track_numberOfWins,
       R7.startType, R7.finishingPosition as finishingPositionUpdated, R7.numberOfStarters, R7.draw, R7.margin, R7.distance, R7.class, R7.handicap, R7.startingPosition,
       R7.odds, R7.winnerOrSecond, R7.positionInRun, R7.time as RunnerRunTime, R7.stewardsComment
from view_Meetings as L 
      LEFT OUTER JOIN view_OneRace as R
       on L.meetingName = R.meetingName and L.meetingDate = R.meetingDate and L.raceType = R.raceType and L.raceNumber = R.raceNumber
      LEFT OUTER JOIN view_OneRaceRunner as R2 
       on L.meetingName = R2.meetingName and L.meetingDate = R2.meetingDate and L.raceNumber = R2.raceNumber and L.raceName = R2.raceName
      LEFT OUTER JOIN view_OneRacePredictionsTemp as R3
       on L.meetingName = R3.meetingName and L.meetingDate = R3.meetingDate and L.raceNumber = R3.raceNumber and L.raceName = R3.raceName and R2.runnerNumber = R3.predictions_runners_runnerNumber
      LEFT OUTER JOIN view_OneRaceRatingsTemp as R4
       on L.meetingName = R4.meetingName and L.meetingDate = R4.meetingDate and L.raceNumber = R4.raceNumber and L.raceName = R4.raceName and R2.runnerNumber = R4.runnerNumber
      LEFT OUTER JOIN view_OneRaceResultRunner as R5
       on L.meetingName = R5.meetingName and L.meetingDate = R5.meetingDate and L.raceNumber = R5.raceNumber and L.raceType = R5.raceType and R2.runnerNumber = R5.runnerNumber       
      LEFT OUTER JOIN view_RunnerStarts as R6
       on L.meetingDate = R6.meetingDate and R2.runnerNumber = R6.runnerNumber and R2.runnerName = R6.runnerName and R2.trainerFullName = R6.trainerName
      LEFT OUTER JOIN view_RunnerPreviousStartsUnique as R7
       on L.meetingDate = R7.startDate and L.raceNumber = R7.raceNumber and R2.runnerNumber = R7.runnerNumber and R2.runnerName = R7.runnerName
      
-- where R3.prediction_Win_probability is not null
--   and L.meetingDate = "2020-06-06"



-- COMMAND ----------

select meetingDate, meetingName, location, raceNumber, runnerName, runnerNumber, 
       finishingPosition,  last5Starts,
       techFormRating, prediction_Place_probability, prediction_Win_probability, fixedOdds_returnWinOpenDaily, fixedOdds_returnWin_close,
       
       numberOfFixedOddsPlaces, numberOfPlaces, prizeMoney, raceClassConditions, raceDistance,  raceStartTime, raceType, 
       tipRunnerNumbers, tipster, tipType, trackCondition, trackDirection, weatherCondition, barrierNumber, dfsFormRating, earlySpeedRating, earlySpeedRatingBand,
       fixedOdds_returnWinOpen,  handicapWeight, riderDriverName, trainerName, 
       
       rating_Class, rating_Distance, rating_Last12Months, rating_Overall, rating_Rating, rating_Recent, rating_Time,

       fixedOdds_placeDeduction, fixedOdds_returnPlace_close, fixedOdds_scratchedTime, resultedTime, 
       age, blinkers, classLevel, colour, dam, daysSinceLastRun, fieldStrength, formComment,
       last20Starts,
       sex, sire, trainerLocation,
       finishingPositionUpdated, 
       numberOfStarters, draw, margin, distance, class, handicap, startingPosition, odds, winnerOrSecond, positionInRun, RunnerRunTime, stewardsComment
from   view_RunnerMasterTemp where meetingDate = "2020-05-29"
--   and  prediction_Win_probability is not null
 and finishingPosition = 1