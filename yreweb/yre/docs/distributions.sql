select count(post_id) from post_favorites_old
	where favorited_user in 
	(select favorited_user from post_favorites_old
	 where post_id=1893674)
-- 503736

select count(distinct post_id) from post_favorites_old
	where favorited_user in 
	(select favorited_user from post_favorites_old
	 where post_id=1893674)
-- 362455

select
	distinct favorited_user as username,
	count(post_id) as favs
from post_favorites_old
	where favorited_user in 
	(select favorited_user from post_favorites_old
	 where post_id=1893674)
group by username order by favs desc
/*
username          |favs |
------------------|-----|
SupremeTempestus  |66047|
Dudeman147        |45236|
insanofrogman     |44433|
KittyCat          |40583|
Dioson            |31662|
addicted1234      |29540|
Duderino          |26857|
Mr-boi1           |25261|
sorenxoras        |20081|
...
*/


select
	distinct post_id,
	count(post_id) as qty
from post_favorites_old
	where favorited_user in 
	(select favorited_user from post_favorites_old
	 where post_id=1893674)
group by post_id order by qty desc
/*
post_id|qty|
-------|---|
1893674| 40|
  59186| 20|
1522407| 14|
1850021| 14|
1910438| 13|
1915868| 13|
1648637| 12|
1810944| 12|
2081830| 12|
1014765| 11|
1201721| 11|
...
*/

select
	distinct qty as qty_bin,
	count(qty) as counts
from
	(select
		distinct post_id,
		count(post_id) as qty
	from post_favorites_old
		where favorited_user in 
		(select favorited_user from post_favorites_old
		 where post_id=1893674)
	group by post_id) as posts_by_overlap
group by qty
/*
qty_bin|counts|
-------|------|
      1|263328|
      2| 70452|
      3| 19758|
      4|  6009|
      5|  1902|
      6|   645|
      7|   222|
      8|    86|
      9|    25|
     10|    12|
     11|     7|
     12|     3|
     13|     2|
     14|     2|
     20|     1|
     40|     1|
*/