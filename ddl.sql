CREATE OR REPLACE TRIGGER PRODUCT_TYPES_BRI_TRI
--  triger used for surogate key generation
  BEFORE INSERT ON PRODUCT_TYPES 
  FOR EACH ROW 
BEGIN 
  SELECT PRODUCT_TYPES_SEQ.NEXTVAL
  INTO :new.PRODUCT_TYPE_ID
  FROM dual;
END;
/

CREATE OR REPLACE TRIGGER PIZZA_TYPES_BRI_TRI
--  triger used for surogate key generation
  BEFORE INSERT ON PIZZA_TYPES 
  FOR EACH ROW 
BEGIN 
  SELECT PIZZA_TYPES_SEQ.NEXTVAL
  INTO :new.PIZZA_TYPE_ID
  FROM dual;
END;
/

CREATE OR REPLACE TRIGGER ORDERS_BRI_TRI
--  triger used for surogate key generation
  BEFORE INSERT ON ORDERS 
  FOR EACH ROW 
BEGIN 
  SELECT ORDERS_SEQ.NEXTVAL
  INTO :new.ORDER_ID
  FROM dual;
END;
/
CREATE OR REPLACE TRIGGER SET_ACTUAL_SALE_PRICE_TRI
--  Trigger used to set pizza price in ORDER_CONTENTS. The ACTUAL_SALE_PRICE is used for pizzaria's profit calculation.
  BEFORE INSERT ON ORDER_CONTENTS
  FOR EACH ROW
BEGIN
    SELECT PRICE
      INTO :NEW.ACTUAL_SALE_PRICE
      FROM PIZZA_TYPES p
      WHERE p.PIZZA_TYPE_ID = :NEW.PIZZA_TYPE_ID;
END;
/
CREATE OR REPLACE TRIGGER ENOUGH_PRODUCTS_FOR_ORDER_TRI
--  Check if there is enough products in warehouse to prepare the inserted order
--  If not lack_of_products_in_warehouse exception is raised
  AFTER INSERT ON ORDER_CONTENTS 
  FOR EACH ROW 
DECLARE
  weight_of_product_to_remove PRODUCTS_IN_WAREHOUSE.WEIGHT%TYPE;
  lack_of_products_in_warehouse EXCEPTION;
  PRAGMA EXCEPTION_INIT(lack_of_products_in_warehouse, -20001);
BEGIN   
  FOR product_used_in_pizza IN 
--  Get product types and their wight which are used to prepere ordered pizza 
    (SELECT PRODUCT_TYPE_ID, WEIGHT
        FROM INGREDIENTS 
        WHERE  PIZZA_TYPE_ID = :NEW.PIZZA_TYPE_ID)
    LOOP
--    Remove used products weight from warehouse and insert into PRODUCTS_USED_IN_ORDER information how much weight and from which delivery were used
      weight_of_product_to_remove := product_used_in_pizza.weight * :NEW.QUANTITY;
--      Get all records which contains current analysed product type and sort them by date - the oldest delivery is first
      FOR product_in_warehouse IN ( 
        SELECT * FROM PRODUCTS_IN_WAREHOUSE p
        WHERE p.PRODUCT_TYPE_ID = product_used_in_pizza.product_type_id
        ORDER BY p.DELIVERY_DATE ASC
        )
      LOOP 
        IF product_in_warehouse.weight >= weight_of_product_to_remove THEN
          INSERT INTO PRODUCTS_USED_IN_ORDER(ORDER_ID, PRODUCT_TYPE_ID, DELIVERY_DATE, USED_WEIGHT, PIZZA_TYPE_ID) 
            VALUES (:NEW.ORDER_ID, product_in_warehouse.product_type_id, product_in_warehouse.delivery_date, 
                    weight_of_product_to_remove, :NEW.PIZZA_TYPE_ID);
          UPDATE PRODUCTS_IN_WAREHOUSE
            SET WEIGHT = WEIGHT - weight_of_product_to_remove
            WHERE PRODUCT_TYPE_ID = product_in_warehouse.product_type_id AND DELIVERY_DATE =  product_in_warehouse.delivery_date;   
          weight_of_product_to_remove :=0;
--         All necessary weight was removed from warehouse. Exit inner loop and go to the next product type
          EXIT;
        END IF;
--        Current analysed record does not contain enough weigh of product. Use all available weight and go to the next product delivery record
        weight_of_product_to_remove := weight_of_product_to_remove - product_in_warehouse.weight;
        INSERT INTO PRODUCTS_USED_IN_ORDER(ORDER_ID, PRODUCT_TYPE_ID, DELIVERY_DATE, USED_WEIGHT, PIZZA_TYPE_ID) 
            VALUES (:NEW.ORDER_ID, product_in_warehouse.product_type_id, product_in_warehouse.delivery_date, 
                    product_in_warehouse.weight, :NEW.PIZZA_TYPE_ID);
        UPDATE PRODUCTS_IN_WAREHOUSE
          SET WEIGHT = 0
            WHERE PRODUCT_TYPE_ID = product_in_warehouse.product_type_id AND DELIVERY_DATE =  product_in_warehouse.delivery_date;
            
      END LOOP product_in_warehouse;
--    Loop went through all warehouse records with the current analysed product type. If weight_of_product_to_remove is bigger than zero, there is not enough product in warehouse to complete the order
      IF weight_of_product_to_remove > 0 THEN
        raise_application_error( -20001, 'There are at least one missing product to procced the order');
        END IF;         
    END LOOP product_used_in_order;
END;
/

CREATE OR REPLACE VIEW PRODUCTS_IN_WAREHOUSE_SUMMARY AS 
  SELECT T1.PRODUCT_TYPE_ID, T1.NAME, SUM(T2.WEIGHT) AS WEIGHT_SUM
--  View which agregates the state of warehouse 
--  Same product type may come from different deliveries (many rows in table). View sums product intances from all deliveries.
    FROM PRODUCT_TYPES T1
    LEFT JOIN PRODUCTS_IN_WAREHOUSE T2
      ON T1.PRODUCT_TYPE_ID = T2.PRODUCT_TYPE_ID 
    GROUP BY T1.PRODUCT_TYPE_ID, T1.NAME;
         
CREATE OR REPLACE VIEW MENU AS 
  SELECT PIZZA_TYPE_ID, NAME, PIZZA_SIZE, PRICE
--  Presents list of pizza on sale (not withdrawn) in alphabethic order
    FROM PIZZA_TYPES
  WHERE WAS_WITHDRAWN = 'N'
  ORDER BY NAME ASC, PIZZA_SIZE ASC;
        
CREATE OR REPLACE VIEW ORDERS_QUEUE AS 
  SELECT T1.ORDER_ID, T1.CREATION_DATE, T3.PIZZA_TYPE_ID, T3.NAME, T3.PIZZA_SIZE
/*  Orders which are not completed yet */
    FROM ORDERS T1
    LEFT JOIN ORDER_CONTENTS T2
      ON T1.ORDER_ID = T2.ORDER_ID
    LEFT JOIN PIZZA_TYPES T3
      ON T2.PIZZA_TYPE_ID = T3.PIZZA_TYPE_ID
    WHERE T1.COMPLETED = 'N';
/
DROP MATERIALIZED VIEW LAST_MONTH_PROFIT_PER_PIZZA;
CREATE MATERIALIZED VIEW LAST_MONTH_PROFIT_PER_PIZZA AS 
  SELECT PIZZA_TYPE_ID, SUM(QUANTITY) AS NUMBER_OF_SOLD, SUM(ACTUAL_SALE_PRICE * QUANTITY) AS INCOME, SUM(PIZZAS_EXPENSES) as EXPENSES, 
/*  view present last month profit and cost for each pizza type */
        SUM(ACTUAL_SALE_PRICE * QUANTITY) - SUM(PIZZAS_EXPENSES) AS PROFIT
        FROM
            (SELECT OCON.PIZZA_TYPE_ID, OCON.ACTUAL_SALE_PRICE, OCON.QUANTITY,
            SUM(PUSED.USED_WEIGHT* PWARE.PRICE) AS PIZZAS_EXPENSES
              FROM ORDERS ORD
              LEFT JOIN ORDER_CONTENTS OCON
                ON ORD.ORDER_ID = OCON.ORDER_ID
              LEFT JOIN PRODUCTS_USED_IN_ORDER PUSED
                ON OCON.ORDER_ID = PUSED.ORDER_ID AND OCON.PIZZA_TYPE_ID = PUSED.PIZZA_TYPE_ID
              LEFT JOIN PRODUCTS_IN_WAREHOUSE PWARE
                ON PUSED.PRODUCT_TYPE_ID = PWARE.PRODUCT_TYPE_ID AND PUSED.DELIVERY_DATE = PWARE.DELIVERY_DATE
--               Ignore empty orders
              WHERE OCON.PIZZA_TYPE_ID IS NOT NULL
                AND ORD.CREATION_DATE > TRUNC(ADD_MONTHS(SYSDATE,-1), 'MONTH') 
                AND ORD.CREATION_DATE < TRUNC(SYSDATE, 'MONTH')
              GROUP BY OCON.ORDER_ID, OCON.PIZZA_TYPE_ID, OCON.ACTUAL_SALE_PRICE, OCON.QUANTITY)  
      GROUP BY PIZZA_TYPE_ID; 
/    
    
CREATE OR REPLACE PROCEDURE print_menu IS
--Create www report whith pizzeria's menu
  CURSOR pizza_cursor IS
    SELECT *
      FROM menu;
BEGIN
--  Display table
  htp.htmlopen;
  htp.headopen;
  htp.title('Menu');
  htp.headclose; 
  htp.bodyopen;
  htp.header(1, 'Pizzas');
  htp.tableOpen(cattributes => 'border=2 width=50%' );
  htp.tableRowOpen;
  htp.tableData('Id');
  htp.tableData('Name');
  htp.tableData('Size');
  htp.tableData('Price');
  htp.tableRowClose;
  FOR pizza IN pizza_cursor LOOP
    htp.tableRowOpen;
    htp.tableData(pizza.pizza_type_id);
    htp.tableData(pizza.name);
    htp.tableData(pizza.pizza_size);
    htp.tableData(pizza.price);
    htp.tableRowClose;
  END LOOP;
  htp.tableClose;
  htp.bodyclose;
  htp.htmlclose;
END;
/

CREATE OR REPLACE PROCEDURE print_available_pizzas IS
-- Create www report with pizzas for which there is enough product in warehouse
  CURSOR pizza_cursor IS
-- Choose pizzas which can be prepared - there is enough product in warehouse to prepare them
SELECT pizza_type_id, name, pizza_size, price 
  FROM(
    SELECT p.pizza_type_id, p.name, p.pizza_size, p.price
      FROM MENU p
      LEFT JOIN ingredients i
        ON i.pizza_type_id = p.pizza_type_id
--        PRODUCTS_IN_WAREHOUSE_SUMMARY contains agregated weigh for each product type
      LEFT JOIN PRODUCTS_IN_WAREHOUSE_SUMMARY w
        ON i.product_type_id = w.product_type_id
      GROUP BY p.pizza_type_id, p.name, p.pizza_size, p.price, i.product_type_id, i.weight, w.PRODUCT_TYPE_ID, w.weight_sum
         HAVING (i.weight<w.weight_sum)
         ) 
  GROUP by pizza_type_id, name, pizza_size, price
  ORDER BY pizza_type_id;
BEGIN
--  Display table
  htp.htmlopen;
  htp.headopen;
  htp.title('Available pizzas');
  htp.headclose; 
  htp.bodyopen;
  htp.header(1, 'Available pizzas');
  htp.tableOpen(cattributes => 'border=2 width=50%' );
  htp.tableRowOpen;
  htp.tableData('Id');
  htp.tableData('Name');
  htp.tableData('Size');
  htp.tableData('Price');
  htp.tableRowClose;
  FOR pizza IN pizza_cursor LOOP
    htp.tableRowOpen;
    htp.tableData(pizza.pizza_type_id);
    htp.tableData(pizza.name);
    htp.tableData(pizza.pizza_size);
    htp.tableData(pizza.price);
    htp.tableRowClose;
  END LOOP;
  htp.tableClose;
  htp.bodyclose;
  htp.htmlclose;
END;

