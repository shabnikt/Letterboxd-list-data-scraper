CREATE SCHEMA letterboxd;


CREATE TABLE letterboxd.film (
  pk_film UUID NOT NULL,
  film_name TEXT NOT NULL,
  film_page TEXT NULL,
  film_img_url TEXT NULL,
  film_streaming TEXT NULL,
  film_watched BOOLEAN NULL DEFAULT FALSE,
  film_use BOOLEAN NULL DEFAULT TRUE,
  PRIMARY KEY (pk_film)
);


CREATE TABLE letterboxd.list (
  pk_list UUID NOT NULL,
  list_name TEXT NOT NULL,
  list_address TEXT NOT NULL,
  list_use BOOLEAN NULL DEFAULT TRUE,
  PRIMARY KEY (pk_list)
);


CREATE TABLE letterboxd.list_content (
  fk_film UUID NOT NULL,
  fk_list UUID NOT NULL,
  PRIMARY KEY (fk_film, fk_list),
  FOREIGN KEY (fk_film) REFERENCES letterboxd.film (pk_film),
  FOREIGN KEY (fk_list) REFERENCES letterboxd.list (pk_list)
);


CREATE TABLE letterboxd.config (
  pk_config UUID NOT NULL,
  random_amount INT NULL,
  PRIMARY KEY (pk_config)
);