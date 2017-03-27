create or replace FUNCTION GetCustomersProductsAll2(
    count_of_orders INT DEFAULT 0)    
  RETURN customers_common_table pipelined
IS
  out_row customers_common_row := customers_common_row(NULL, NULL, NULL, NULL, NULL);
  i NUMBER(8,2):=0;
BEGIN
  FOR customer_iterator IN
  (
      select CUST_ID, CUST_NAME
      from CUSTOMERS
  )
  LOOP

     select NVL(count(o.CUST_ID), 0) into i
     FROM CUSTOMERS c
        left join ORDERS o on c.CUST_ID = o.CUST_ID
     WHERE c.CUST_ID =customer_iterator.CUST_ID
     GROUP BY c.CUST_ID;

     if i >= count_of_orders then
        for order_iterator in
        (
          select ORDER_NUM from ORDERS where CUST_ID = customer_iterator.CUST_ID
        )
        loop
            out_row.cust_name   := customer_iterator.CUST_NAME;
            out_row.cust_id     := customer_iterator.CUST_ID;
            out_row.order_num   := order_iterator.ORDER_NUM;
            out_row.product_name:= NULL;
            out_row.product_id  := NULL;
            pipe row(out_row);
        end loop;
     else
        for product_iterator in
        (
          select PROD_ID, PROD_NAME from PRODUCTS
        )
        loop
            out_row.cust_name   := customer_iterator.CUST_NAME;
            out_row.cust_id     := customer_iterator.CUST_ID;
            out_row.order_num   := NULL;
            out_row.product_name:= product_iterator.PROD_NAME;
            out_row.product_id  := product_iterator.PROD_ID;
            pipe row(out_row);
        end loop;
     end if;
  END LOOP;
END;

select * from GetCustomersProductsAll2(1);