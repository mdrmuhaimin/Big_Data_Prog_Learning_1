DROP TABLE customer;
DROP TABLE nation ;
DROP TABLE orders ;
DROP TABLE partsupp ;
DROP TABLE part;
DROP TABLE region ;
DROP TABLE supplier;
DROP TABLE lineitem;
DROP TABLE orders_parts;

CREATE TABLE customer (
  custkey int,
  name text,
  address text,
  nationkey int,
  phone text,
  acctbal decimal,
  mktsegment text,
  comment text,
  PRIMARY KEY (custkey)
);
CREATE TABLE nation (
  nationkey int,
  name text,
  regionkey int,
  comment text,
  PRIMARY KEY (nationkey)
);
CREATE TABLE orders (
  orderkey int,
  custkey int,
  orderstatus text,
  totalprice decimal,
  orderdate text,
  order_priority text,
  clerk text,
  ship_priority int,
  comment text,
  PRIMARY KEY (orderkey)
);
CREATE TABLE orders_parts (
  part_names set<text>,
  orderkey int,
  custkey int,
  orderstatus text,
  totalprice decimal,
  orderdate text,
  order_priority text,
  clerk text,
  ship_priority int,
  comment text,
  PRIMARY KEY (orderkey)
);
CREATE TABLE partsupp (
  partkey int,
  suppkey int,
  availqty int,
  supplycost decimal,
  comment text,
  PRIMARY KEY (partkey, suppkey)
);
CREATE TABLE part (
  partkey int,
  name text,
  mfgr text,
  brand text,
  type text,
  size int,
  container text,
  retailprice decimal,
  comment text,
  PRIMARY KEY (partkey)
);
CREATE TABLE region (
  regionkey int,
  name text,
  comment text,
  PRIMARY KEY (regionkey)
);
CREATE TABLE supplier (
  suppkey int,
  name text,
  address text,
  nationkey int,
  phone text,
  acctbal decimal,
  comment text,
  PRIMARY KEY (suppkey)
);
CREATE TABLE lineitem (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity int,
  extendedprice decimal,
  discount decimal,
  tax decimal,
  returnflag text,
  linestatus text,
  shipdate text,
  commitdate text,
  receiptdate text,
  shipinstruct text,
  shipmode text,
  comment text,
  PRIMARY KEY (linenumber, orderkey)
);