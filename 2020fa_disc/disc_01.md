# DIS 1

## 1. Single-Table SQL

(a)

```sql
SELECT SONG_ID
FROM Songs
ORDER BY weeks_in_top_40, song_name
LIMIT 5
```

(b)

```sql
SELECT artist_name, first_yr_active
FROM Artists
WHERE artist_name like 'B%'
```

(c)

```sql
SELECT genre, count(*) 
FROM Albums
GROUP BY genre
```

(d)

```sql
SELECT genre, count(*) 
FROM Albums
GROUP BY genre
having count(*) >= 10 
```

(e)

```sql
SELECT genre
FROM Albums
WHERE yr_releASed = 2000
GROUP BY genre
ORDER BY count(*) desc 
LIMIT 1 -- OFFSET 1 if want secONd
```

## 2. Multi-Table SQL

(a)

```sql

SELECT a1.artist_name
FROM Artists AS a1
JOIN Albums AS a2 ON a1.ARTIST_ID = a2.artist_id
WHERE a2.r_releASed = 2020 and a2.genre = 'country'
GROUP BY a2.artist_id, a1.artist_name -- If an artist publishes multiple country albums in 2020, 
-- we need to make sure the artist_name appears ONly ONce in the results.
-- add GROUP BY a1.artist_name because we need to SELECT it, so make it aggregatiON.
```

(b)

```sql
SELECT a.album_name
FROM Albums AS a
JOIN Songs AS s ON a.ALBUM_ID = s.album_id
ORDER BY s.weeks_in_top_40
LIMIT 1
```

(c)

```sql
SELECT a1.artist_name, max(s.weeks_in_top_40)
FROM Artists AS a1 
LEFT OUT JOIN Albums AS a2 ON a1.ARTIST_ID = a2.artist_id
LEFT OUT JOIN Songs AS s ON a2.ALBUM_ID = s.album_id
GROUP BY a1.ARTIST_ID, a1.artist_name -- same reason in (a)
```
