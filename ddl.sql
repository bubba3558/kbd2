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

CREATE OR REPLACE TRIGGER ENOUGH_PRODUCTS_FOR_ORDER_TRI
--  check if there is enough products in warehouse to prepare the inserted order
--  if not lack_of_products_in_warehouse exception is raised
  AFTER INSERT ON ORDER_CONTENTS 
  FOR EACH ROW 
DECLARE
  weight_of_product_to_remove PRODUCTS_IN_WAREHOUSE.WEIGHT%TYPE;
  lack_of_products_in_warehouse EXCEPTION;
  PRAGMA EXCEPTION_INIT(lack_of_products_in_warehouse, -20001);
BEGIN   
  FOR product_used_in_pizza IN 
    (SELECT PRODUCT_TYPE_ID, WEIGHT
        FROM INGREDIENTS 
        WHERE  PIZZA_TYPE_ID = :NEW.PIZZA_TYPE_ID)
    LOOP
      weight_of_product_to_remove := product_used_in_pizza.weight;
      FOR product_in_warehouse IN ( 
        SELECT * FROM PRODUCTS_IN_WAREHOUSE p
        WHERE p.PRODUCT_TYPE_ID = product_used_in_pizza.PRODUCT_TYPE_ID
        ORDER BY p.DELIVERY_DATE ASC
        )
      LOOP 
        IF product_in_warehouse.weight >= weight_of_product_to_remove THEN
          UPDATE PRODUCTS_IN_WAREHOUSE
            SET WEIGHT = WEIGHT - weight_of_product_to_remove
            WHERE PRODUCT_TYPE_ID = product_in_warehouse.product_type_id AND DELIVERY_DATE =  product_in_warehouse.delivery_date;
      
          INSERT INTO PRODUCTS_USED_IN_ORDER(ORDER_ID, PRODUCT_TYPE_ID, DELIVERY_DATE, WEIGHT_USED_IN_ORDER, PIZZA_TYPE_ID) 
            VALUES (NEW.ORDER_ID, product_in_warehouse.product_type_id, product_in_warehouse.product_type_id.delivery_date, 
                    weight_of_product_to_remove, NEW.PIZZA_TYPE_ID);
            weight_of_product_to_remove :=0;
          EXIT;
        END IF;
        
        weight_of_product_to_remove := weight_of_product_to_remove - product_in_warehouse.weight;
        INSERT INTO PRODUCTS_USED_IN_ORDER(ORDER_ID, PRODUCT_TYPE_ID, DELIVERY_DATE, WEIGHT_USED_IN_ORDER, PIZZA_TYPE_ID) 
            VALUES (NEW.ORDER_ID, product_in_warehouse.product_type_id, product_in_warehouse.product_type_id.delivery_date, 
                    product_in_warehouse.weight, NEW.PIZZA_TYPE_ID);
        UPDATE PRODUCTS_IN_WAREHOUSE
          SET WEIGHT = 0
            WHERE PRODUCT_TYPE_ID = product_in_warehouse.product_type_id AND DELIVERY_DATE =  product_in_warehouse.delivery_date;
      END LOOP product_in_warehouse;
      IF weight_of_product_to_remove > 0 THEN
        raise_application_error( -20001, 'There are at least one missing product to procced the order');
        END IF;         
    END LOOP product_used_in_order;
END;
/

CREATE VIEW PRODUCTS_IN_WAREHOUSE_SUMARY AS 
--  View which agregates the state of warehouse 
--  Same product type may come from different deliveries (many rows in table). View sums product intances from all deliveries.
  SELECT T1.PRODUCT_TYPE_ID, T1.NAME, SUM(T2.WEIGHT) AS WEIGHT_SUM
    FROM PRODUCT_TYPES T1
    LEFT JOIN PRODUCTS_IN_WAREHOUSE T2
      ON T1.PRODUCT_TYPE_ID = T2.PRODUCT_TYPE_ID 
    GROUP BY T1.PRODUCT_TYPE_ID, T1.NAME;
         
CREATE OR REPLACE VIEW MENU AS 
--  Presents list of pizza on sale (not withdrawn) in alphabethic order
  SELECT PIZZA_TYPE_ID, NAME, PIZZA_SIZE, PRICE
    FROM PIZZA_TYPES
  WHERE WAS_WITHDRAWN = 'N'
  ORDER BY NAME ASC, PIZZA_SIZE ASC;
        
CREATE OR REPLACE VIEW ORDERS_QUEUE AS 
--  Orders which are not complated yet
  SELECT T1.ORDER_ID, T1.CREATION_DATE, T3.PIZZA_TYPE_ID, T3.NAME, T3.PIZZA_SIZE
    FROM ORDERS T1
    LEFT JOIN ORDER_CONTENTS T2
      ON T1.ORDER_ID = T2.ORDER_ID
    LEFT JOIN PIZZA_TYPES T3
      ON T2.PIZZA_TYPE_ID = T3.PIZZA_TYPE_ID
    WHERE T1.COMPLETED = 'N';
        
CREATE MATERIALIZED VIEW LAST_MONTH_PROFIT_PER_PIZZA AS 
--  view present profit and cost for each pizza type
  SELECT PIZZA_TYPE_ID, SUM(PIZZA_PRICE_DURING_ORDER) AS INCOME, SUM(ONE_PIZZA_EXPENSES) as EXPENSES, 
        SUM(PIZZA_PRICE_DURING_ORDER) - SUM(ONE_PIZZA_EXPENSES) AS PROFIT 
        FROM
          (SELECT OCON.PIZZA_TYPE_ID, OCON.PIZZA_PRICE_DURING_ORDER,
            SUM(PUSED.USED_WEIGHT* PWARE.PRICE) AS ONE_PIZZA_EXPENSES
              FROM ORDERS ORD
              LEFT JOIN ORDER_CONTENTS OCON
                ON ORD.ORDER_ID = OCON.ORDER_ID
              LEFT JOIN PRODUCTS_USED_IN_ORDER PUSED
                ON OCON.ORDER_ID = PUSED.ORDER_ID AND OCON.PIZZA_TYPE_ID = PUSED.PIZZA_TYPE_ID
              LEFT JOIN PRODUCTS_IN_WAREHOUSE PWARE
                ON PUSED.PRODUCT_TYPE_ID = PWARE.PRODUCT_TYPE_ID AND PUSED.DELIVERY_DATE = PWARE.DELIVERY_DATE
              WHERE ORD.CREATION_DATE > TRUNC(ADD_MONTHS(SYSDATE,-1), 'MONTH') AND ORD.CREATION_DATE < TRUNC(SYSDATE, 'MONTH')
              GROUP BY OCON.ORDER_ID, OCON.PIZZA_TYPE_ID, OCON.PIZZA_PRICE_DURING_ORDER)  
      GROUP BY PIZZA_TYPE_ID;
/    
    
