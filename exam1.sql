--create row type
create or replace type customer_row is object(
cust_name char(50),
cust_id char(10),
order_num number(38, 0)
);

--create table1 type
create or replace type customers_table_info is table of customer_row;

--function1
create or replace function GetCustomers(count_of_orders INT default 0)
return customers_table_info
pipelined IS
  out_row customer_row := customer_row(null, null, null);
 BEGIN
    FOR customer_iterator IN (
      SELECT c.CUST_NAME, c.CUST_ID, o.ORDER_NUM
      FROM CUSTOMERS c
        LEFT JOIN ORDERS o ON c.CUST_ID = o.CUST_ID
        inner join(
             SELECT fc.CUST_ID
             FROM CUSTOMERS fc
                  JOIN ORDERS fo ON fc.CUST_ID = fo.CUST_ID
             GROUP BY fc.CUST_ID
             HAVING COUNT(fo.CUST_ID)>=count_of_orders
            ) f on f.CUST_ID = c.CUST_ID
    )
    LOOP
      out_row.cust_name:= customer_iterator.CUST_NAME;
      out_row.cust_id:= customer_iterator.CUST_ID;
      out_row.order_num:= customer_iterator.ORDER_NUM;
      pipe row(out_row);
    END LOOP;
END;

--create row type
create or replace type customerProduct_row is object(
cust_name char(50),
cust_id char(10),
product_name char(255),
product_id char(10)
);

--create table2 type
create or replace type customersProduct_table is table of customerProduct_row;

--function 2
CREATE OR REPLACE FUNCTION GetCustomersProducts(
    count_of_orders INT DEFAULT 0)
  RETURN customersProduct_table pipelined
IS
  out_row customerProduct_row := customerProduct_row(NULL, NULL, NULL, NULL);
BEGIN
  FOR customer_iterator IN
  (
    SELECT c.CUST_NAME, c.CUST_ID, p.PROD_NAME, p.PROD_ID
    FROM
      CUSTOMERS c
        CROSS JOIN PRODUCTS p
        LEFT JOIN
              (
                SELECT CUSTOMERS.CUST_ID
                FROM CUSTOMERS
                  LEFT JOIN ORDERS ON CUSTOMERS.CUST_ID = ORDERS.CUST_ID
                GROUP BY CUSTOMERS.CUST_NAME, CUSTOMERS.CUST_ID
                HAVING COUNT(ORDERS.CUST_ID) >= count_of_orders
              )
        f ON f.CUST_ID = c.CUST_ID
    WHERE
      f.CUST_ID IS NULL
  )
  LOOP
    out_row.cust_name   := customer_iterator.CUST_NAME;
    out_row.cust_id     := customer_iterator.CUST_ID;
    out_row.product_name:=customer_iterator.PROD_NAME;
    out_row.product_id  :=customer_iterator.PROD_ID;
    pipe row(out_row);
  END LOOP;
END;

--common row-type
create or replace type customers_common_row is object(
  cust_name char(50),
  cust_id char(10),
  order_num number(38, 0),
  product_name char(255),
  product_id char(10)
);

--common table-type
create or replace type customers_common_table is table of customers_common_row;

CREATE OR REPLACE FUNCTION GetCustomersProductsAll(
    count_of_orders INT DEFAULT 0)
  RETURN customers_common_table pipelined
IS
  out_row customers_common_row := customers_common_row(NULL, NULL, NULL, NULL, NULL);
BEGIN
  FOR customer_iterator IN
  (
      select CUST_NAME, CUST_ID, ORDER_NUM, NULL as PRODUCT_NAME, NULL as PRODUCT_ID
      from TABLE(GetCustomers(count_of_orders))
      union all
      select CUST_NAME, CUST_ID, NULL, PRODUCT_NAME, PRODUCT_ID
      from TABLE(GetCustomersProducts(count_of_orders))
  )
  LOOP
    out_row.cust_name   := customer_iterator.CUST_NAME;
    out_row.cust_id     := customer_iterator.CUST_ID;
    out_row.order_num   := customer_iterator.ORDER_NUM;
    out_row.product_name:= customer_iterator.PRODUCT_NAME;
    out_row.product_id  := customer_iterator.PRODUCT_ID;
    pipe row(out_row);
  END LOOP;
END;

--select * from TABLE(GetCustomers(1));
--select * from TABLE(GetCustomersProducts(1));
select * from TABLE(GetCustomersProductsAll(1));