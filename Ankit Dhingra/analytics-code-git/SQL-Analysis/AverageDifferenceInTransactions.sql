SELECT userId,AVG(TimeDiff) AverageDiffTimeSeconds,
COUNT(1) TotalRecords
FROM
(SELECT firstuserId,firsttransaction,firstproduct, nextproduct, TIMESTAMP_DIFF(nextTS,firstTS, SECOND) TimeDiff
FROM
(SELECT firstHit.userId firstuserId,
        firstHit.transactionId firsttransaction,
        nextHit.transactionId nexttransaction,
        firstHit.productId firstproduct,
        nextHit.productId nextproduct,
        firstHit.transactionTime firstTS,
        nexthit.transactionTime nextTS
FROM (SELECT userId,transactionTime,transactionId,productId, RANK() OVER (PARTITION BY userId ORDER BY transactionTime ASC) AS RN 
           FROM dataset.transactionTable
           WHERE _PARTITIONTIME = TIMESTAMP("2018-02-26")
           AND gameId = "80715286"
           AND sdkv = "8.8.1"
           GROUP BY userId,transactionTime,transactionId,productId) firstHit
INNER JOIN (SELECT userId,transactionTime,transactionId,productId, RANK() OVER (PARTITION BY userId ORDER BY transactionTime ASC) AS RN 
           FROM dataset.transactionTable
           WHERE _PARTITIONTIME = TIMESTAMP("2018-02-26")
           AND gameId = "80715286"
           AND sdkv = "8.8.1"
           GROUP BY userId,transactionTime,transactionId,productId) nextHit ON firstHit.userId = nextHit.userId AND nextHit.transactionTime > firstHit.transactionTime AND (nextHit.RN - firstHit.RN  = 1)
GROUP BY firstuserId,firsttransaction,nexttransaction,firstTS,nextTS,firstproduct, nextproduct)
GROUP BY firstuserId,firsttransaction,nextTS,firstTS,firstproduct, nextproduct
HAVING TimeDiff <= 900)
GROUP BY userId 