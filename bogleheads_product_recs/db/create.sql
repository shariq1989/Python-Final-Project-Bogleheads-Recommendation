CREATE TABLE threads(
	id 		        SERIAL 	PRIMARY KEY,
	thread_id	    INT NOT NULL,
	title	        TEXT,
);

CREATE TABLE links(
	id 		        SERIAL	PRIMARY KEY,
	url		        TEXT NOT NULL,
	thread_id	    INT REFERENCES threads(id),
	page            INT,
    domain          TEXT
);