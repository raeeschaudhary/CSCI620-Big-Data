DROP SCHEMA IF EXISTS sql_exercises CASCADE;
CREATE SCHEMA sql_exercises;
SET search_path TO sql_exercises;
CREATE TABLE customers(
   customerid INTEGER  NOT NULL PRIMARY KEY 
  ,firstname  VARCHAR(50) NOT NULL
  ,lastname   VARCHAR(50) NOT NULL
  ,city       VARCHAR(50) NOT NULL
  ,state      VARCHAR(50) NOT NULL
);
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10101,'John','Gray','Lynden','Washington');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10298,'Leroy','Brown','Pinetop','Arizona');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10299,'Elroy','Keller','Snoqualmie','Washington');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10315,'Lisa','Jones','Oshkosh','Wisconsin');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10325,'Ginger','Schultz','Pocatello','Idaho');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10329,'Kelly','Mendoza','Kailua','Hawaii');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10330,'Shawn','Dalton','Cannon Beach','Oregon');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10338,'Michael','Howell','Tillamook','Oregon');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10339,'Anthony','Sanchez','Winslow','Arizona');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10408,'Elroy','Cleaver','Globe','Arizona');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10410,'03y Ann','Howell','Charleston','South Carolina');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10413,'Donald','Davids','Gila Bend','Arizona');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10419,'Linda','Sakahara','Nogales','Arizona');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10429,'Sarah','Graham','Greensboro','North Carolina');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10438,'Kevin','Smith','Durango','Colorado');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10439,'Conrad','Giles','Telluride','Colorado');
INSERT INTO customers(customerid,firstname,lastname,city,state) VALUES (10449,'Isabela','Moore','Yuma','Arizona');
CREATE TABLE items_ordered(
   customerid INTEGER  NOT NULL
  ,order_date DATE  NOT NULL
  ,item       VARCHAR(50) NOT NULL
  ,quantity   INTEGER  NOT NULL
  ,price      NUMERIC(6,2) NOT NULL
  ,PRIMARY KEY(customerid,order_date)
  ,FOREIGN KEY(customerid) REFERENCES customers(customerid)
);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10330,'1999-06-30','Pogo stick',1,28);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'1999-06-30','Raft',1,58);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10298,'1999-07-01','Skateboard',1,33);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'1999-07-01','Life Vest',4,125);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10299,'1999-07-06','Parachute',1,1250);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10339,'1999-07-27','Umbrella',1,4.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'1999-08-13','Unicycle',1,180.79);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10439,'1999-08-14','Ski Poles',2,25.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'1999-08-18','Rain Coat',1,18.3);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'1999-09-01','Snow Shoes',1,45);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10439,'1999-09-18','Tent',1,88);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10298,'1999-09-19','Lantern',2,29);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10410,'1999-10-28','Sleeping Bag',1,89.22);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10438,'1999-11-01','Umbrella',1,6.75);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10438,'1999-11-02','Pillow',1,8.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10298,'1999-12-01','Helmet',1,22);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'1999-12-15','Bicycle',1,380.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'1999-12-22','Canoe',1,280);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'1999-12-30','Hoola Hoop',3,14.75);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10330,'2000-01-01','Flashlight',4,28);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'2000-01-02','Lantern',1,16);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10299,'2000-01-18','Inflatable Mattress',1,38);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10438,'2000-01-18','Tent',1,79.99);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10413,'2000-01-19','Lawnchair',4,32);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10410,'2000-01-30','Unicycle',1,192.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10315,'2000-02-02','Compass',1,8);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'2000-02-29','Flashlight',1,4.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10101,'2000-03-08','Sleeping Bag',2,88.7);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10298,'2000-03-18','Pocket Knife',1,22.38);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10449,'2000-03-19','Canoe paddle',2,40);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10298,'2000-04-01','Ear Muffs',1,12.5);
INSERT INTO items_ordered(customerid,order_date,item,quantity,price) VALUES (10330,'2000-04-19','Shovel',1,16.75);
