-- Databricks notebook source
describe delta.`/mnt/gamble/DELTA/SILVER/DATA/RunnerPreviousStarts`

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
;

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

select * from view_RunnerStarts

-- COMMAND ----------

Create OR Replace temporary view view_RunnerMasterTemp AS

select L.allIn, L.allowBundle, R.allowFixedOddsPlace, R.allowMulti, R.allowParimutuelPlace, R.apprenticesCanClaim, R.audio, R.betTypes, R.broadcastChannel, R.broadcastChannels,
       R.cashOutEligibility, R.fixedOddsOnlineBetting, R.fixedOddsUpdateTime, R.hasEarlySpeedRatings, R.hasFixedOdds, R.hasForm, R.hasParimutuel, R.location, R.meetingDate,
       R.meetingName, R.multiLegApproximates, R.numberOfFixedOddsPlaces, R.numberOfPlaces, R.oddsUpdateTime, R.parimutuelPlaceStatus, R.pools, R.predictions,
       R.previewVideo, R.prizeMoney, R.raceClassConditions, R.raceDistance, R.raceName, R.raceNumber, R.raceStartTime, R.raceStatus, R.raceType, L.railPosition,
       R.ratings, R.results, R.runners, L.scratchings, R.sellCode_meetingCode, R.sellCode_scheduledType, L.skyRacing_audio, L.skyRacing_video, R.substitute, R.tipRunnerNumbers,
       R.tipster, R.tipType, L.trackCondition, R.trackDirection, R.venueMnemonic, L.weatherCondition, R.willHaveFixedOdds, R2.barrierNumber, R2.claimAmount, R2.dfsFormRating, 
       R2.earlySpeedRating, R2.earlySpeedRatingBand, R2.emergency, R2.fixedOdds_allowPlace, R2.fixedOdds_bettingStatus, R2.fixedOdds_differential, R2.fixedOdds_flucs, R2.fixedOdds_isFavouritePlace,
       R2.fixedOdds_isFavouriteWin, R2.fixedOdds_percentageChange, R2.fixedOdds_propositionNumber, R2.fixedOdds_returnPlace, R2.fixedOdds_returnWin, R2.fixedOdds_returnWinOpen, 
       R2.fixedOdds_returnWinOpenDaily, R2.fixedOdds_returnWinTime, R2.handicapWeight, R2.harnessHandicap, R2.last5Starts, R2.parimutuel_bettingStatus, R2.parimutuel_isFavouriteExact2, 
       R2.parimutuel_isFavouritePlace, R2.parimutuel_isFavouriteWin, R2.parimutuel_marketMovers, R2.parimutuel_percentageChange, R2.parimutuel_returnExact2, R2.parimutuel_returnPlace, 
       R2.parimutuel_returnWin, R2.penalty, R2.riderDriverFullName, R2.riderDriverName, R2.runnerName, R2.runnerNumber, R2.silkURL, R2.tcdwIndicators, R2.techFormRating,
       R2.totalRatingPoints, R2.trainerFullName, R2.trainerName, R2.vacantBox, R3.prediction_Place_probability, R3.prediction_Win_probability,
       R4.rating_Class, R4.rating_Distance, R4.rating_Last12Months, R4.rating_Overall, R4.rating_Rating, R4.rating_Recent, R4.rating_Time,
       R5.finishingPosition, R5.fixedOdds_placeDeduction, R5.fixedOdds_returnPlace as fixedOdds_returnPlace_close, R5.fixedOdds_returnWin as fixedOdds_returnWin_close,
       R5.fixedOdds_scratchedTime, R5.fixedOdds_winDeduction, R5.parimutuel_returnPlace as parimutuel_returnPlace_close, R5.parimutuel_returnWin as parimutuel_returnWin_close,
       R5.resultedTime, R5.results, R5.winBook, R6.age, R6.blinkers, R6.classLevel, R6.colour, R6.dam, R6.daysSinceLastRun, R6.fieldStrength, R6.formComment, R6.formComments,
       R6.last20Starts, R6.riderDriverStarts_last12Months_numberOfPlacings, R6.riderDriverStarts_last12Months_numberOfStarts, R6.riderDriverStarts_last12Months_numberOfWins,
       R6.riderDriverStarts_last30Days_numberOfPlacings, R6.riderDriverStarts_last30Days_numberOfStarts, R6.riderDriverStarts_last30Days_numberOfWins, R6.riderDriverStarts_region_numberOfPlacings,
       R6.riderDriverStarts_region_numberOfStarts, R6.riderDriverStarts_region_numberOfWins, R6.riderDriverStarts_runner_numberOfPlacings, R6.riderDriverStarts_runner_numberOfStarts,
       R6.riderDriverStarts_runner_numberOfWins, R6.riderDriverStarts_track_numberOfPlacings, R6.riderDriverStarts_track_numberOfStarts, R6.riderDriverStarts_track_numberOfWins,
       R6.riderOrDriver, R6.riderOrDriverSex,
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
       on L.meetingDate = R6.meetingDate and R2.runnerNumber = R6.runnerNumber and R2.runnerName = R6.runnerName and R2.trainerName = R6.trainerName
-- where R3.prediction_Win_probability is not null
--   and L.meetingDate = "2020-06-06"



-- COMMAND ----------

select * from view_RunnerMasterTemp where meetingDate = "2020-06-13"