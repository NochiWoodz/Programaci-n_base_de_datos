SHOW USER
SET SERVEROUTPUT ON
---inicio caso 1
-- Declaración de variables bind (SIN ; al final)
VARIABLE p_run         VARCHAR2(15)
VARIABLE p_peso_normal NUMBER
VARIABLE p_limite1     NUMBER
VARIABLE p_limite2     NUMBER
VARIABLE p_extra1      NUMBER
VARIABLE p_extra2      NUMBER
VARIABLE p_extra3      NUMBER

-- Asignación de valores (puede ser con EXEC)
EXEC :p_peso_normal := 1200
EXEC :p_limite1     := 1000000
EXEC :p_limite2     := 3000000
EXEC :p_extra1      := 100
EXEC :p_extra2      := 300
EXEC :p_extra3      := 550
-- Cambiar segun rut de cliente
EXEC :p_run := '22176845-2'

DECLARE
  v_nro_cliente     cliente.nro_cliente%TYPE;
  v_nombre_cliente  VARCHAR2(50);
  v_tipo_cliente    VARCHAR2(30);

  v_inicio_anio_ant DATE;
  v_inicio_anio_act DATE;

  v_suma_montos     NUMBER := 0;
  v_pesos_total     NUMBER := 0;
  v_extra_unit      NUMBER := 0;

  v_es_independiente BOOLEAN := FALSE;
BEGIN
  SELECT
    c.nro_cliente,
    TRIM(REGEXP_REPLACE(
      c.pnombre || ' ' || NVL(c.snombre,'') || ' ' || c.appaterno || ' ' || NVL(c.apmaterno,''),
      ' +',' '
    )),
    tc.nombre_tipo_cliente
  INTO
    v_nro_cliente,
    v_nombre_cliente,
    v_tipo_cliente
  FROM cliente c
  JOIN tipo_cliente tc ON tc.cod_tipo_cliente = c.cod_tipo_cliente
  WHERE (TO_CHAR(c.numrun) || '-' || c.dvrun) = :p_run;

  v_inicio_anio_act := TRUNC(SYSDATE, 'YYYY');
  v_inicio_anio_ant := ADD_MONTHS(v_inicio_anio_act, -12);

  SELECT NVL(SUM(cc.monto_solicitado), 0)
  INTO v_suma_montos
  FROM credito_cliente cc
  WHERE cc.nro_cliente = v_nro_cliente
    AND cc.fecha_otorga_cred >= v_inicio_anio_ant
    AND cc.fecha_otorga_cred <  v_inicio_anio_act;

  IF v_suma_montos = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'El cliente no tiene créditos otorgados el año anterior.');
  END IF;

  v_es_independiente := (UPPER(v_tipo_cliente) LIKE '%INDEPENDIENTE%');

  IF v_es_independiente THEN
    IF v_suma_montos < :p_limite1 THEN
      v_extra_unit := :p_extra1;
    ELSIF v_suma_montos <= :p_limite2 THEN
      v_extra_unit := :p_extra2;
    ELSE
      v_extra_unit := :p_extra3;
    END IF;
  ELSE
    v_extra_unit := 0;
  END IF;

  FOR r IN (
    SELECT cc.monto_solicitado
    FROM credito_cliente cc
    WHERE cc.nro_cliente = v_nro_cliente
      AND cc.fecha_otorga_cred >= v_inicio_anio_ant
      AND cc.fecha_otorga_cred <  v_inicio_anio_act
  ) LOOP
    v_pesos_total :=
      v_pesos_total
      + FLOOR(r.monto_solicitado / 100000) * (:p_peso_normal + v_extra_unit);
  END LOOP;

  INSERT INTO cliente_todosuma
    (nro_cliente, run_cliente, nombre_cliente, tipo_cliente, monto_solic_creditos, monto_pesos_todosuma)
  VALUES
    (v_nro_cliente, :p_run, v_nombre_cliente, v_tipo_cliente, v_suma_montos, v_pesos_total);

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('OK -> RUN: ' || :p_run
                       || ' | Monto créditos año anterior: ' || v_suma_montos
                       || ' | Pesos TODOSUMA: ' || v_pesos_total);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20002, 'RUN no encontrado en CLIENTE: ' || :p_run);
  WHEN DUP_VAL_ON_INDEX THEN
    RAISE_APPLICATION_ERROR(-20003,
      'El cliente ya está en CLIENTE_TODOSUMA. Elimínalo antes de re-ejecutar para el mismo RUN.');
END;
/

--- revision de resultado
SELECT * FROM cliente_todosuma WHERE run_cliente = '22176845-2';

SELECT *
FROM cliente_todosuma
ORDER BY nro_cliente;

---fin caso 1

---inicio caso 2
SHOW USER;
SET SERVEROUTPUT ON;

-- ver información clientes
SELECT
  c.nro_cliente,
  TO_CHAR(c.numrun) || '-' || c.dvrun AS run,
  TRIM(REGEXP_REPLACE(
    c.pnombre || ' ' || NVL(c.snombre,'') || ' ' || c.appaterno || ' ' || NVL(c.apmaterno,''),
    ' +',' '
  )) AS nombre_completo
FROM cliente c
WHERE UPPER(c.pnombre) IN ('SEBASTIAN','KAREN','JULIAN')
ORDER BY c.pnombre, c.appaterno;
--inicio---
-- PARÁMETROS (BIND VARIABLES)

VAR p_nro_cliente       NUMBER;
VAR p_nro_solic_credito NUMBER;
VAR p_cant_postergar    NUMBER;

EXEC :p_nro_cliente := 13;
EXEC :p_nro_solic_credito := 2004;
EXEC :p_cant_postergar := 1;




DECLARE
  -- Datos del crédito
  v_nombre_credito  credito.nombre_credito%TYPE;

  -- Última cuota original del crédito
  v_ult_nro_cuota   cuota_credito_cliente.nro_cuota%TYPE;
  v_ult_fecha_venc  cuota_credito_cliente.fecha_venc_cuota%TYPE;
  v_ult_valor_cuota cuota_credito_cliente.valor_cuota%TYPE;

  -- Nuevas cuotas
  v_nuevo_nro_cuota   NUMBER;
  v_nueva_fecha_venc  DATE;
  v_nuevo_valor_cuota NUMBER;

  -- Control año anterior (automático)
  v_inicio_anio_act DATE;
  v_inicio_anio_ant DATE;

  -- Regla condonación (más de 1 crédito el año anterior)
  v_cant_creditos_anio_ant NUMBER := 0;

  -- Tasa a aplicar según reglas del caso (en decimal)
  v_tasa NUMBER := 0;
BEGIN
 
  -- 1) Validar que el crédito pertenece al cliente y obtener tipo de crédito (SQL)

  SELECT cr.nombre_credito
    INTO v_nombre_credito
    FROM credito_cliente cc
    JOIN credito cr ON cr.cod_credito = cc.cod_credito
   WHERE cc.nro_solic_credito = :p_nro_solic_credito
     AND cc.nro_cliente       = :p_nro_cliente;

  -- 2) Obtener última cuota original (SQL)

  SELECT MAX(cu.nro_cuota)
    INTO v_ult_nro_cuota
    FROM cuota_credito_cliente cu
   WHERE cu.nro_solic_credito = :p_nro_solic_credito;

  SELECT cu.fecha_venc_cuota, cu.valor_cuota
    INTO v_ult_fecha_venc, v_ult_valor_cuota
    FROM cuota_credito_cliente cu
   WHERE cu.nro_solic_credito = :p_nro_solic_credito
     AND cu.nro_cuota         = v_ult_nro_cuota;

  -- 3) Determinar tasa según tipo de crédito y cantidad a postergar (PL/SQL con IF)
 
  IF UPPER(v_nombre_credito) LIKE '%HIPOTEC%' THEN
    IF :p_cant_postergar = 1 THEN
      v_tasa := 0;        -- 1 cuota sin interés
    ELSIF :p_cant_postergar = 2 THEN
      v_tasa := 0.005;    -- 0,5% sobre el valor de la última cuota
    ELSE
      RAISE_APPLICATION_ERROR(-20010,
        'Hipotecario: solo se permite postergar 1 o 2 cuotas.');
    END IF;

  ELSIF UPPER(v_nombre_credito) LIKE '%CONSUMO%' THEN
    IF :p_cant_postergar = 1 THEN
      v_tasa := 0.01;     -- 1% sobre el valor de la última cuota
    ELSE
      RAISE_APPLICATION_ERROR(-20011,
        'Consumo: solo se permite postergar 1 cuota.');
    END IF;

  ELSIF UPPER(v_nombre_credito) LIKE '%AUTOMOTRIZ%' THEN
    IF :p_cant_postergar = 1 THEN
      v_tasa := 0.02;     -- 2% sobre el valor de la última cuota
    ELSE
      RAISE_APPLICATION_ERROR(-20012,
        'Automotriz: solo se permite postergar 1 cuota.');
    END IF;

  ELSE
    RAISE_APPLICATION_ERROR(-20013,
      'Este tipo de crédito no está considerado para postergación en este caso.');
  END IF;

  -- 4) Calcular rango del año anterior (PL/SQL)

  v_inicio_anio_act := TRUNC(SYSDATE, 'YYYY');         -- 01-01 del año actual
  v_inicio_anio_ant := ADD_MONTHS(v_inicio_anio_act, -12); -- 01-01 del año anterior

  -- 5) Verificar si el cliente tuvo más de 1 crédito el año anterior (SQL)

  SELECT COUNT(*)
    INTO v_cant_creditos_anio_ant
    FROM credito_cliente cc
   WHERE cc.nro_cliente = :p_nro_cliente
     AND cc.fecha_otorga_cred >= v_inicio_anio_ant
     AND cc.fecha_otorga_cred <  v_inicio_anio_act;

  -- Si tuvo más de un crédito el año anterior, la última cuota ORIGINAL queda pagada
  IF v_cant_creditos_anio_ant > 1 THEN

    -- 6) Marcar como pagada la última cuota original (SQL)

    UPDATE cuota_credito_cliente cu
       SET cu.fecha_pago_cuota = cu.fecha_venc_cuota,
           cu.monto_pagado     = cu.valor_cuota
     WHERE cu.nro_solic_credito = :p_nro_solic_credito
       AND cu.nro_cuota         = v_ult_nro_cuota;
  END IF;

  -- 7) Generar nuevas cuotas automáticamente (PL/SQL + INSERT SQL)

  FOR i IN 1 .. :p_cant_postergar LOOP
    v_nuevo_nro_cuota  := v_ult_nro_cuota + i;
    v_nueva_fecha_venc := ADD_MONTHS(v_ult_fecha_venc, i);

    -- Valor cuota con interés (valor_cuota es NUMBER(10), por eso redondeamos)
    v_nuevo_valor_cuota := ROUND(v_ult_valor_cuota * (1 + v_tasa));

    INSERT INTO cuota_credito_cliente
      (nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado, saldo_por_pagar, cod_forma_pago)
    VALUES
      (:p_nro_solic_credito, v_nuevo_nro_cuota, v_nueva_fecha_venc, v_nuevo_valor_cuota,
       NULL, NULL, NULL, NULL);
  END LOOP;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('OK -> Cliente: ' || :p_nro_cliente ||
                       ' | Crédito: ' || :p_nro_solic_credito ||
                       ' | Postergadas: ' || :p_cant_postergar ||
                       ' | Tipo: ' || v_nombre_credito ||
                       ' | Tasa aplicada: ' || (v_tasa*100) || '%');

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20020,
      'No se encontró el crédito para ese cliente (verifica nro_cliente y nro_solic_credito).');
END;
/

--Revision cuotas nuevas
SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado, saldo_por_pagar, cod_forma_pago
FROM cuota_credito_cliente
WHERE nro_solic_credito = 2001
ORDER BY nro_cuota;

SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado, saldo_por_pagar, cod_forma_pago
FROM cuota_credito_cliente
WHERE nro_solic_credito = 3004
ORDER BY nro_cuota;

SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado, saldo_por_pagar, cod_forma_pago
FROM cuota_credito_cliente
WHERE nro_solic_credito = 2004
ORDER BY nro_cuota;

---verificación ultima cuota pagada

SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado
FROM cuota_credito_cliente
WHERE nro_solic_credito = 2001
  AND nro_cuota = (SELECT MAX(nro_cuota) - 2 FROM cuota_credito_cliente WHERE nro_solic_credito = 2001);

SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado
FROM cuota_credito_cliente
WHERE nro_solic_credito = 3004
  AND nro_cuota = (SELECT MAX(nro_cuota) - 1 FROM cuota_credito_cliente WHERE nro_solic_credito = 3004);

SELECT nro_solic_credito, nro_cuota, fecha_venc_cuota, valor_cuota,
       fecha_pago_cuota, monto_pagado
FROM cuota_credito_cliente
WHERE nro_solic_credito = 2004
  AND nro_cuota = (SELECT MAX(nro_cuota) - 1 FROM cuota_credito_cliente WHERE nro_solic_credito = 2004);
