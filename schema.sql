DROP SCHEMA IF EXISTS films CASCADE;


create schema films;
drop table if exists films.film_info CASCADE;
drop table if exists films.crew_cast CASCADE;
drop table if exists films.income CASCADE;
drop table if exists films.ranking CASCADE;
drop table if exists films.drama CASCADE;
drop table if exists films.action CASCADE;
drop table if exists films.adventure CASCADE;
drop table if exists films.crime CASCADE;
drop table if exists films.comedy CASCADE;
drop table if exists films.biography CASCADE;
drop table if exists films.other_genre CASCADE;

CREATE TABLE films.film_info (
  "title" varchar PRIMARY KEY,
  "year" int,
  "genre" varchar,
  "duration" int,
  "rating" float
);

CREATE TABLE films.crew_cast (
  "title" varchar references films.film_info("title"),
  "director" varchar,
  "cast" varchar,
  "distributor" varchar
);

CREATE TABLE films.income (
  "title" varchar references films.film_info("title"),
  "domestic_k" int,
  "international_k" int,
  "worldwide_k" int,
  "dom_pct" float,
  "int_pct" float
);

CREATE TABLE films.ranking (
  "title" varchar references films.film_info("title"),
  "place" int,
  "duration_rank" float,
  "income_rank" float,
  "rank_diff" float
);

CREATE TABLE films.drama (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.action (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.adventure (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.crime (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.comedy (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.biography (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);

CREATE TABLE films.other_genre (
  "title" varchar references films.film_info("title"),
  "place_bygenre" float,
  "duration_rank_bygenre" float,
  "income_rank_bygenre" float
);
