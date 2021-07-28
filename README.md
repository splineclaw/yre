## Y R E
![Example output](https://raw.githubusercontent.com/splineclaw/yre/master/example.jpg)
_Source image by gugu-troll. Suggested works by shlinka, wolnir, angiewolf, amenlona, koul, melloque, and alaiaorax._

YRE (pronounced `yuri`) is a favorites-based recommendation engine which provides suggestions for related posts on e621.

It provides a Django-based web interface by which users may explore related posts.

- [Y R E](#y-r-e)
  - [Dependencies](#dependencies)
  - [Use](#use)
  - [URLs](#urls)


### Dependencies

- psycopg2-binary
- Django


### Use

This project is very much a work in progress.

The database is updated by running `yreweb/yre/database.py`. 

Launch the web interface by running `python manage.py runserver` in the root directory.


### URLs

| Endpoint | Function |
|---|---|
| / | Shows similars of a default image. |
| /\<id\>/ | Shows similars of post \<id\>. |
| /recompute/\<id\>/ | Recompute similars of post \<id\>. |
| /full/\<id\>/ | Recompute similars of post \<id\> using all favorites (not a subset). |
| /subset/ | Force resampling of favorites subset. |
| /urls/\<id\>/ | Get URLs only of similars' sample images. |
| /tuple/\<id\>/ | Get a tuple of similars' IDs. |


