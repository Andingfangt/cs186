-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
-- Find the namefirst, namelast and birthyear for all players with weight greater than 300 pounds.
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT p.namefirst, p.namelast, p.birthyear
  FROM people AS p
  WHERE p.weight > 300
;

-- Question 1ii
-- Find the namefirst, namelast and birthyear of all players whose namefirst field contains a space. Order the results by namefirst, breaking ties with namelast both in ascending order
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER BY namefirst ASC, namelast ASC
;

-- Question 1iii
-- group together players with the same birthyear, and report the birthyear, average height, and number of players for each birthyear.
-- Order the results by birthyear in ascending order.
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear ASC
;

-- Question 1iv
-- Following the results of part iii, now only include groups with an average height > 70. Again order the results by birthyear in ascending order.
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avgheight, count
  FROM q1iii
  WHERE avgheight > 70
  ORDER BY birthyear ASC
;

-- Question 2i
-- Find the namefirst, namelast, playerid and yearid of all people who were successfully inducted into the Hall of Fame in descending order of yearid.
-- Break ties on yearid by playerid (ascending).
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerID, h.yearid
  FROM people AS p, HallofFame AS h
  WHERE p.playerID = h.playerID AND h.inducted = 'Y'
  ORDER BY h.yearid DESC, p.playerid ASC
;

-- Question 2ii
-- Find the people who were successfully inducted into the Hall of Fame and played in college at a school located in the state of California.
-- For each person, return their namefirst, namelast, playerid, schoolid, and yearid in descending order of yearid.
-- Break ties on yearid by schoolid, playerid (ascending). For this question, yearid refers to the year of induction into the Hall of Fame.
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerID, s.schoolID, h.yearid
  FROM HallofFame AS h, Schools AS s, CollegePlaying AS c, people AS p
  WHERE p.playerID = h.playerID AND h.inducted = 'Y' AND h.playerID = c.playerID AND c.schoolID = s.schoolID AND s.schoolState = 'CA'
  ORDER BY h.yearid DESC, s.schoolID ASC, p.playerID ASC
;

-- Question 2iii
-- Find the playerid, namefirst, namelast and schoolid of all people who were successfully inducted into the Hall of Fame whether or not they played in college.
-- Return people in descending order of playerid. Break ties on playerid by schoolid (ascending). (Note: schoolid should be NULL if they did not play in college.)
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT q2i.playerid, q2i.namefirst, q2i.namelast, c.schoolID
  FROM q2i
  LEFT OUTER JOIN CollegePlaying AS c ON q2i.playerid = c.playerID
  ORDER BY q2i.playerid DESC, c.schoolID ASC
;

-- Question 3i
-- Find the playerid, namefirst, namelast, yearid and single-year slg (Slugging Percentage) of the players with the 10 best annual Slugging Percentage recorded over all time.
-- A player can appear multiple times in the output. For example, if Babe Ruth’s slg in 2000 and 2001 both landed in the top 10 best annual Slugging Percentage of all time,
-- then we should include Babe Ruth twice in the output. For statistical significance, only include players with more than 50 at-bats in the season.
-- Order the results by slg descending, and break ties by yearid, playerid (ascending).
DROP VIEW IF EXISTS slg;
create view slg(playerid, yearid,slg, AB)
as
    select playerID, yearID, (H + H2B + 2*H3B + 3*HR + 0.0)/ (AB+ 0.0), AB
    from batting
;

CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
SELECT  p."playerID"
       ,p."nameFirst"
       ,p."nameLast"
       ,slg.yearid
       ,slg.slg
FROM people AS p
INNER JOIN slg
ON p.playerID = slg.playerid
WHERE slg.AB > 50
ORDER BY slg.slg DESC, slg.yearid, p."playerID"
LIMIT 10
;



-- Question 3ii
-- Following the results from Part i, find the playerid, namefirst, namelast and lslg (Lifetime Slugging Percentage)
-- for the players with the top 10 Lifetime Slugging Percentage. Lifetime Slugging Percentage (LSLG) uses the same formula as Slugging Percentage (SLG),
-- but it uses the number of singles, doubles, triples, home runs, and at bats each player has over their entire career, rather than just over a single season.
DROP VIEW IF EXISTS lslg;
create view lslg(playerid,lslg)
AS
  SELECT  playerID
         ,(SUM(H) + SUM(H2B) + 2* SUM(H3B) + 3* SUM(HR) + 0.0 ) / (SUM(AB) )
  FROM batting
  GROUP BY  "playerID"
  HAVING SUM(AB) > 50
;



CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p."playerID"
        ,p."nameFirst"
        ,p."nameLast"
        ,lslg.lslg
  FROM people AS p
  INNER JOIN lslg
  ON p."playerID" = lslg.playerid
  ORDER BY lslg.lslg desc, p."playerID"
  LIMIT 10
;

-- Question 3iii
-- Find the namefirst, namelast and Lifetime Slugging Percentage (lslg) of batters whose lifetime slugging percentage is higher
-- than that of San Francisco favorite Willie Mays.
DROP VIEW IF EXISTS will_may;
create view will_may(lslg)
as
  SELECT  (SUM(H) + SUM(H2B) + 2* SUM(H3B) + 3* SUM(HR) + 0.0 ) / (SUM(AB) )
  FROM batting
  WHERE "playerID" = 'mayswi01'
;
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT  p."nameFirst"
        ,p."nameLast"
        ,lslg.lslg
  FROM people AS p
  INNER JOIN lslg
  ON p."playerID" = lslg.playerid, will_may AS w
  WHERE lslg.lslg > w.lslg
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT  yearID
        ,MIN(salary)
        ,MAX(salary)
        ,AVG(salary)
  FROM salaries
  GROUP BY  "yearID"
  ORDER BY  "yearID"
;

-- Question 4ii
DROP VIEW IF EXISTS MM2016;
CREATE view MM2016(min, max, width) 
AS
  SELECT min
        ,max
        ,(max-min+ 0.0)/ 10
  FROM q4i
  WHERE yearid = '2016'
;



CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT  b.binid
        ,m.min+m.width*binid
        ,m.min+m.width*(binid+1)
        ,COUNT(*)
  FROM binids AS b, salaries AS s, MM2016 AS m
  WHERE s."yearID" = '2016'
  AND (s.salary BETWEEN m.min+m.width*binid AND m.min+m.width*(binid+1))
  GROUP BY b.binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT t1.yearid
        ,t1.min - t2.min
        ,t1.max - t2.max
        ,t1.avg - t2.avg
  FROM q4i AS t1, q4i AS t2
  WHERE t1.yearid = t2.yearid + 1
  GROUP BY t1.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT  p."playerID"
        ,p."nameFirst"
        ,p."nameLast"
        ,s.salary
        ,s."yearID"
  FROM people AS p
  JOIN salaries AS s ON p."playerID" = s."playerID"
  JOIN q4i AS m
  ON s."yearID" = m.yearid
  WHERE s.salary = m."max"
  AND m.yearid IN (2000, 2001)
;



-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT  a."teamID"
        ,MAX(s.salary) - MIN(s.salary)
  FROM allstarfull AS a
  INNER JOIN salaries AS s
  ON a."playerID" = s."playerID" AND a."yearID" = s."yearID" AND a."yearID" = 2016
  GROUP BY  a."teamID"
;



