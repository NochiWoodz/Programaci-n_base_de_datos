SET SERVEROUTPUT ON;

-- Santiago Vivanco Espinoza

--------------------------------------------------------------------------------
-- CASO 1 - CIRCULO ALL THE BEST (CATB)
--------------------------------------------------------------------------------

-- Tramos paramétricos (BIND)
VARIABLE b_tramo1_desde NUMBER;
VARIABLE b_tramo1_hasta NUMBER;
VARIABLE b_tramo2_desde NUMBER;
VARIABLE b_tramo2_hasta NUMBER;
VARIABLE b_tramo3_desde NUMBER;

BEGIN
  :b_tramo1_desde := 500000;
  :b_tramo1_hasta := 700000;

  :b_tramo2_desde := 700001;
  :b_tramo2_hasta := 900000;

  :b_tramo3_desde := 900001;
END;
/
PRINT b_tramo1_desde
PRINT b_tramo1_hasta
PRINT b_tramo2_desde
PRINT b_tramo2_hasta
PRINT b_tramo3_desde

--------------------------------------------------------------------------------
-- BLOQUE PL/SQL ANÓNIMO
--------------------------------------------------------------------------------
DECLARE
  ------------------------------------------------------------------------------
  -- 1) Fechas del año anterior (SIN fechas fijas)
  ------------------------------------------------------------------------------
  v_inicio_anno_ant DATE;
  v_inicio_anno_act DATE;

  ------------------------------------------------------------------------------
  -- 2) VARRAY de puntos
  ------------------------------------------------------------------------------
  TYPE t_puntos IS VARRAY(4) OF NUMBER;
  v_puntos t_puntos := t_puntos(250, 300, 550, 700);

  ------------------------------------------------------------------------------
  -- 3) RECORD para manejar el detalle
  ------------------------------------------------------------------------------
  TYPE r_detalle IS RECORD (
    numrun            DETALLE_PUNTOS_TARJETA_CATB.numrun%TYPE,
    dvrun             DETALLE_PUNTOS_TARJETA_CATB.dvrun%TYPE,
    nro_tarjeta       DETALLE_PUNTOS_TARJETA_CATB.nro_tarjeta%TYPE,
    nro_transaccion   DETALLE_PUNTOS_TARJETA_CATB.nro_transaccion%TYPE,
    fecha_trans       DETALLE_PUNTOS_TARJETA_CATB.fecha_transaccion%TYPE,
    tipo_trans        DETALLE_PUNTOS_TARJETA_CATB.tipo_transaccion%TYPE,
    monto_trans       DETALLE_PUNTOS_TARJETA_CATB.monto_transaccion%TYPE,
    mes_anno          VARCHAR2(6),
    tipo_cliente      VARCHAR2(50),
    monto_total_anual NUMBER
  );
  v_det r_detalle;

  ------------------------------------------------------------------------------
  -- 4) Cursor sin parámetro: REF CURSOR (variable de cursor)
  ------------------------------------------------------------------------------
  TYPE t_refcur IS REF CURSOR;
  c_detalle t_refcur;

  ------------------------------------------------------------------------------
  -- 5) Cursor con parámetro: Cursor explícito para resumen por mes
  ------------------------------------------------------------------------------
CURSOR c_resumen(p_mes_anno VARCHAR2) IS
  SELECT
    p_mes_anno AS mes_anno,

    -- COMPRAS
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'COMPRA%' THEN monto_transaccion
        ELSE 0
      END
    ) AS monto_total_compras,
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'COMPRA%' THEN puntos_allthebest
        ELSE 0
      END
    ) AS total_puntos_compras,

    -- AVANCES (solo los que empiezan con AVANCE)
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'AVANCE%' THEN monto_transaccion
        ELSE 0
      END
    ) AS monto_total_avances,
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'AVANCE%' THEN puntos_allthebest
        ELSE 0
      END
    ) AS total_puntos_avances,

    -- SÚPER AVANCES (empieza con S y contiene AVANCE) -> funciona aunque el acento esté roto
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'S%' AND INSTR(UPPER(tipo_transaccion), 'AVANCE') > 0
          THEN monto_transaccion
        ELSE 0
      END
    ) AS monto_total_savances,
    SUM(
      CASE
        WHEN UPPER(tipo_transaccion) LIKE 'S%' AND INSTR(UPPER(tipo_transaccion), 'AVANCE') > 0
          THEN puntos_allthebest
        ELSE 0
      END
    ) AS total_puntos_savances

  FROM DETALLE_PUNTOS_TARJETA_CATB
  WHERE TO_CHAR(fecha_transaccion, 'MMYYYY') = p_mes_anno;



  ------------------------------------------------------------------------------
  -- 6) Set de meses (para no repetir) + lista de meses (para recorrer)
  ------------------------------------------------------------------------------
  TYPE t_set_mes IS TABLE OF NUMBER INDEX BY VARCHAR2(6);
  v_meses_set t_set_mes;

  TYPE t_lista_mes IS TABLE OF VARCHAR2(6);
  v_lista_meses t_lista_mes := t_lista_mes();

  ------------------------------------------------------------------------------
  -- 7) Variables de tramos y cálculo
  ------------------------------------------------------------------------------
  v_tramos_1_desde NUMBER := :b_tramo1_desde;
  v_tramos_1_hasta NUMBER := :b_tramo1_hasta;
  v_tramos_2_desde NUMBER := :b_tramo2_desde;
  v_tramos_2_hasta NUMBER := :b_tramo2_hasta;
  v_tramos_3_desde NUMBER := :b_tramo3_desde;

  v_factor_100k     NUMBER;
  v_extra_por_100k  NUMBER;
  v_puntos_base     NUMBER;
  v_puntos_extra    NUMBER;
  v_puntos_total    NUMBER;

  v_mes VARCHAR2(6);

  -- Para ordenar meses en PL/SQL
  v_tmp VARCHAR2(6);

BEGIN
  ------------------------------------------------------------------------------
  -- A) Rango de fechas: año anterior completo
  ------------------------------------------------------------------------------
  v_inicio_anno_act := TRUNC(SYSDATE, 'YYYY');
  v_inicio_anno_ant := ADD_MONTHS(v_inicio_anno_act, -12);

  DBMS_OUTPUT.PUT_LINE('Procesando transacciones desde '||TO_CHAR(v_inicio_anno_ant,'DD/MM/YYYY')
                       ||' hasta antes de '||TO_CHAR(v_inicio_anno_act,'DD/MM/YYYY'));

  ------------------------------------------------------------------------------
  -- B) TRUNCATE tablas de salida
  ------------------------------------------------------------------------------
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';

  ------------------------------------------------------------------------------
  -- C) Abrir REF CURSOR sin parámetro
  ------------------------------------------------------------------------------
  OPEN c_detalle FOR
    SELECT
      c.numrun,
      c.dvrun,
      t.nro_tarjeta,
      tr.nro_transaccion,
      tr.fecha_transaccion,
      tt.nombre_tptran_tarjeta AS tipo_transaccion,
      tr.monto_transaccion,
      TO_CHAR(tr.fecha_transaccion, 'MMYYYY') AS mes_anno,
      tc.nombre_tipo_cliente AS tipo_cliente,
      SUM(tr.monto_transaccion) OVER (PARTITION BY c.numrun) AS monto_total_anual
    FROM CLIENTE c
    JOIN TARJETA_CLIENTE t
      ON t.numrun = c.numrun
    JOIN TRANSACCION_TARJETA_CLIENTE tr
      ON tr.nro_tarjeta = t.nro_tarjeta
    JOIN TIPO_TRANSACCION_TARJETA tt
      ON tt.cod_tptran_tarjeta = tr.cod_tptran_tarjeta
    JOIN TIPO_CLIENTE tc
      ON tc.cod_tipo_cliente = c.cod_tipo_cliente
    WHERE tr.fecha_transaccion >= v_inicio_anno_ant
      AND tr.fecha_transaccion <  v_inicio_anno_act
    ORDER BY tr.fecha_transaccion, c.numrun, tr.nro_transaccion;

  ------------------------------------------------------------------------------
  -- D) Recorrer transacciones -> calcular puntos en PL/SQL -> insertar DETALLE
  ------------------------------------------------------------------------------
  LOOP
    FETCH c_detalle INTO
      v_det.numrun, v_det.dvrun, v_det.nro_tarjeta, v_det.nro_transaccion,
      v_det.fecha_trans, v_det.tipo_trans, v_det.monto_trans,
      v_det.mes_anno, v_det.tipo_cliente, v_det.monto_total_anual;

    EXIT WHEN c_detalle%NOTFOUND;

    -- Guardar mes único
    IF NOT v_meses_set.EXISTS(v_det.mes_anno) THEN
      v_meses_set(v_det.mes_anno) := 1;
      v_lista_meses.EXTEND;
      v_lista_meses(v_lista_meses.COUNT) := v_det.mes_anno;
    END IF;

    v_factor_100k := TRUNC(v_det.monto_trans / 100000);
    v_puntos_base := v_factor_100k * v_puntos(1);

    v_extra_por_100k := 0;

    IF (UPPER(v_det.tipo_cliente) LIKE 'DUE%' OR UPPER(v_det.tipo_cliente) LIKE 'PENSION%') THEN
      IF v_det.monto_total_anual BETWEEN v_tramos_1_desde AND v_tramos_1_hasta THEN
        v_extra_por_100k := v_puntos(2);
      ELSIF v_det.monto_total_anual BETWEEN v_tramos_2_desde AND v_tramos_2_hasta THEN
        v_extra_por_100k := v_puntos(3);
      ELSIF v_det.monto_total_anual >= v_tramos_3_desde THEN
        v_extra_por_100k := v_puntos(4);
      END IF;
    END IF;

    v_puntos_extra := v_factor_100k * v_extra_por_100k;
    v_puntos_total := v_puntos_base + v_puntos_extra;

    INSERT INTO DETALLE_PUNTOS_TARJETA_CATB
      (numrun, dvrun, nro_tarjeta, nro_transaccion, fecha_transaccion,
       tipo_transaccion, monto_transaccion, puntos_allthebest)
    VALUES
      (v_det.numrun, v_det.dvrun, v_det.nro_tarjeta, v_det.nro_transaccion, v_det.fecha_trans,
       SUBSTR(v_det.tipo_trans,1,40), v_det.monto_trans, v_puntos_total);

  END LOOP;

  CLOSE c_detalle;

  ------------------------------------------------------------------------------
  -- E) ORDENAR MESES EN PL/SQL (sin usar SQL sobre la colección)
  ------------------------------------------------------------------------------
  IF v_lista_meses.COUNT > 1 THEN
    FOR i IN 1 .. v_lista_meses.COUNT - 1 LOOP
      FOR j IN i + 1 .. v_lista_meses.COUNT LOOP
        IF v_lista_meses(i) > v_lista_meses(j) THEN
          v_tmp := v_lista_meses(i);
          v_lista_meses(i) := v_lista_meses(j);
          v_lista_meses(j) := v_tmp;
        END IF;
      END LOOP;
    END LOOP;
  END IF;

  ------------------------------------------------------------------------------
  -- F) Generar RESUMEN con cursor explícito con parámetro
  ------------------------------------------------------------------------------
  FOR i IN 1 .. v_lista_meses.COUNT LOOP
    v_mes := v_lista_meses(i);

    FOR r IN c_resumen(v_mes) LOOP
      INSERT INTO RESUMEN_PUNTOS_TARJETA_CATB
        (mes_anno,
         monto_total_compras, total_puntos_compras,
         monto_total_avances, total_puntos_avances,
         monto_total_savances, total_puntos_savances)
      VALUES
        (r.mes_anno,
         NVL(r.monto_total_compras,0), NVL(r.total_puntos_compras,0),
         NVL(r.monto_total_avances,0), NVL(r.total_puntos_avances,0),
         NVL(r.monto_total_savances,0), NVL(r.total_puntos_savances,0));
    END LOOP;
  END LOOP;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('OK: Proceso terminado. Revisa DETALLE_PUNTOS_TARJETA_CATB y RESUMEN_PUNTOS_TARJETA_CATB.');

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERROR: '||SQLERRM);
END;
/
--------------------------------------------------------------------------------
-- Consultas rápidas de validación
--------------------------------------------------------------------------------
SELECT COUNT(*) AS total_filas_detalle FROM DETALLE_PUNTOS_TARJETA_CATB;

SELECT * FROM RESUMEN_PUNTOS_TARJETA_CATB
ORDER BY mes_anno;

SELECT * FROM DETALLE_PUNTOS_TARJETA_CATB
ORDER BY fecha_transaccion, numrun, nro_transaccion;

--------------------------------------------------------------------------------
--Validaciones caso 1
--------------------------------------------------------------------------------
---Compras
SELECT SUM(monto_total_compras) monto, SUM(total_puntos_compras) puntos
FROM RESUMEN_PUNTOS_TARJETA_CATB;

---Avances
SELECT SUM(monto_total_avances) monto, SUM(total_puntos_avances) puntos
FROM RESUMEN_PUNTOS_TARJETA_CATB;

---Super avances
SELECT SUM(monto_total_savances) monto, SUM(total_puntos_savances) puntos
FROM RESUMEN_PUNTOS_TARJETA_CATB;

SET SERVEROUTPUT ON;

--------------------------------------------------------------------------------
-- CASO 2 - APORTE SBIF (Avances y Súper Avances)
-- Ejecutar con F5 (Run Script) en SQL Developer
--------------------------------------------------------------------------------

DECLARE
  ------------------------------------------------------------------------------
  -- 1) Rango de fechas: AÑO ACTUAL (mismo año de ejecución)
  --    Si hoy estás en 2026, procesa 01/01/2026 al 31/12/2026.
  ------------------------------------------------------------------------------
  v_inicio_anno DATE;
  v_fin_anno    DATE;

  ------------------------------------------------------------------------------
  -- 2) Cursor explícito SIN parámetro: detalle (Avances + Súper Avances)
  --    Orden requerido: fecha_transaccion, numrun
  ------------------------------------------------------------------------------
  CURSOR c_detalle IS
    SELECT
      c.numrun,
      c.dvrun,
      t.nro_tarjeta,
      tr.nro_transaccion,
      tr.fecha_transaccion,
      SUBSTR(tt.nombre_tptran_tarjeta,1,40) AS tipo_transaccion,
      tr.monto_total_transaccion            AS monto_total_con_interes, -- ya viene con tasa aplicada
      TO_CHAR(tr.fecha_transaccion,'MMYYYY') AS mes_anno
    FROM CLIENTE c
    JOIN TARJETA_CLIENTE t
      ON t.numrun = c.numrun
    JOIN TRANSACCION_TARJETA_CLIENTE tr
      ON tr.nro_tarjeta = t.nro_tarjeta
    JOIN TIPO_TRANSACCION_TARJETA tt
      ON tt.cod_tptran_tarjeta = tr.cod_tptran_tarjeta
    WHERE tr.fecha_transaccion >= v_inicio_anno
      AND tr.fecha_transaccion <  v_fin_anno
      -- Solo Avances y Súper Avances:
      AND (
           UPPER(tt.nombre_tptran_tarjeta) LIKE 'AVANCE%' OR
           (UPPER(tt.nombre_tptran_tarjeta) LIKE 'S%' AND INSTR(UPPER(tt.nombre_tptran_tarjeta),'AVANCE') > 0)
          )
    ORDER BY tr.fecha_transaccion, c.numrun;

  ------------------------------------------------------------------------------
  -- 3) Cursor explícito CON parámetro: resumen por mes y tipo
  --    (Se alimenta desde la tabla DETALLE_APORTE_SBIF ya cargada)
  ------------------------------------------------------------------------------
  CURSOR c_resumen(p_mes_anno VARCHAR2, p_tipo VARCHAR2) IS
    SELECT
      p_mes_anno AS mes_anno,
      p_tipo     AS tipo_transaccion,
      SUM(monto_transaccion) AS monto_total_transacciones,
      SUM(aporte_sbif)       AS aporte_total_abif
    FROM DETALLE_APORTE_SBIF
    WHERE TO_CHAR(fecha_transaccion,'MMYYYY') = p_mes_anno
      AND tipo_transaccion = p_tipo;

  ------------------------------------------------------------------------------
  -- 4) Estructuras para guardar combinaciones únicas (MES + TIPO) para resumen
  ------------------------------------------------------------------------------
  TYPE t_set_key IS TABLE OF NUMBER INDEX BY VARCHAR2(200);
  v_set_keys t_set_key;

  TYPE t_list_key IS TABLE OF VARCHAR2(200);
  v_list_keys t_list_key := t_list_key();

  ------------------------------------------------------------------------------
  -- 5) Variables para cálculo de aporte
  ------------------------------------------------------------------------------
  v_porc_aporte NUMBER;
  v_aporte      NUMBER;

  ------------------------------------------------------------------------------
  -- 6) Variables auxiliares para ordenamiento
  ------------------------------------------------------------------------------
  v_tmp_key  VARCHAR2(200);
  v_mes      VARCHAR2(6);
  v_tipo     VARCHAR2(40);

  -- Para partir key en mes|tipo
  v_pos_sep  NUMBER;

BEGIN
  ------------------------------------------------------------------------------
  -- A) Definir año actual (sin fechas fijas)
  ------------------------------------------------------------------------------
  v_inicio_anno := TRUNC(SYSDATE,'YYYY');
  v_fin_anno    := ADD_MONTHS(v_inicio_anno, 12);

  DBMS_OUTPUT.PUT_LINE(
    'Procesando Avances y Súper Avances desde '||TO_CHAR(v_inicio_anno,'DD/MM/YYYY')||
    ' hasta antes de '||TO_CHAR(v_fin_anno,'DD/MM/YYYY')
  );

  ------------------------------------------------------------------------------
  -- B) TRUNCATE en tiempo de ejecución (permite re-ejecutar)
  ------------------------------------------------------------------------------
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';

  ------------------------------------------------------------------------------
  -- C) Recorrer detalle, calcular aporte en PL/SQL, insertar DETALLE
  ------------------------------------------------------------------------------
  FOR r IN c_detalle LOOP

    -- Buscar % de aporte según tramo (tabla TRAMO_APORTE_SBIF)
    -- (Esto NO es el cálculo en el SELECT, se hace en PL/SQL)
    SELECT porc_aporte_sbif
      INTO v_porc_aporte
      FROM TRAMO_APORTE_SBIF
     WHERE r.monto_total_con_interes BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;

    -- Cálculo del aporte (con tasa aplicada ya incluida en monto_total_con_interes)
    -- Redondeo a entero (puedes cambiar a TRUNC si tu profe lo pide)
    v_aporte := ROUND(r.monto_total_con_interes * (v_porc_aporte / 100));

    -- Insertar DETALLE (monto_transaccion = MONTO TOTAL con interés, según regla negocio)
    INSERT INTO DETALLE_APORTE_SBIF
      (numrun, dvrun, nro_tarjeta, nro_transaccion, fecha_transaccion,
       tipo_transaccion, monto_transaccion, aporte_sbif)
    VALUES
      (r.numrun, r.dvrun, r.nro_tarjeta, r.nro_transaccion, r.fecha_transaccion,
       r.tipo_transaccion, r.monto_total_con_interes, v_aporte);

    -- Guardar clave única para resumen: mes|tipo
    -- (Así luego generamos RESUMEN en el orden pedido)
    v_tmp_key := r.mes_anno || '|' || r.tipo_transaccion;

    IF NOT v_set_keys.EXISTS(v_tmp_key) THEN
      v_set_keys(v_tmp_key) := 1;
      v_list_keys.EXTEND;
      v_list_keys(v_list_keys.COUNT) := v_tmp_key;
    END IF;

  END LOOP;

  ------------------------------------------------------------------------------
  -- D) Ordenar claves en PL/SQL (mes asc, luego tipo asc)
  ------------------------------------------------------------------------------
  IF v_list_keys.COUNT > 1 THEN
    FOR i IN 1 .. v_list_keys.COUNT - 1 LOOP
      FOR j IN i + 1 .. v_list_keys.COUNT LOOP
        IF v_list_keys(i) > v_list_keys(j) THEN
          v_tmp_key := v_list_keys(i);
          v_list_keys(i) := v_list_keys(j);
          v_list_keys(j) := v_tmp_key;
        END IF;
      END LOOP;
    END LOOP;
  END IF;

  ------------------------------------------------------------------------------
  -- E) Generar RESUMEN usando el cursor con parámetro (mes, tipo)
  --    Orden requerido: mes_anno asc y tipo_transaccion
  ------------------------------------------------------------------------------
  FOR i IN 1 .. v_list_keys.COUNT LOOP

    v_pos_sep := INSTR(v_list_keys(i), '|');
    v_mes  := SUBSTR(v_list_keys(i), 1, v_pos_sep - 1);
    v_tipo := SUBSTR(v_list_keys(i), v_pos_sep + 1);

    FOR rs IN c_resumen(v_mes, v_tipo) LOOP
      INSERT INTO RESUMEN_APORTE_SBIF
        (mes_anno, tipo_transaccion, monto_total_transacciones, aporte_total_abif)
      VALUES
        (rs.mes_anno, rs.tipo_transaccion,
         NVL(rs.monto_total_transacciones,0),
         NVL(rs.aporte_total_abif,0));
    END LOOP;

  END LOOP;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('OK: Proceso terminado. Revisa DETALLE_APORTE_SBIF y RESUMEN_APORTE_SBIF.');

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERROR: No se encontró tramo en TRAMO_APORTE_SBIF para algún monto.');
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERROR: '||SQLERRM);
END;
/
--------------------------------------------------------------------------------
-- Consultas rápidas para validar
--------------------------------------------------------------------------------
SELECT COUNT(*) AS total_filas_detalle
FROM DETALLE_APORTE_SBIF;

SELECT *
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;

SELECT *
FROM DETALLE_APORTE_SBIF
ORDER BY fecha_transaccion, numrun;

---Total aporte detalle = total aporte resumen
SELECT
  (SELECT SUM(aporte_sbif) FROM DETALLE_APORTE_SBIF) AS total_aporte_detalle,
  (SELECT SUM(aporte_total_abif) FROM RESUMEN_APORTE_SBIF) AS total_aporte_resumen
FROM dual;
---Total monto detalle = total monto resumen
SELECT
  (SELECT SUM(monto_transaccion) FROM DETALLE_APORTE_SBIF) AS total_monto_detalle,
  (SELECT SUM(monto_total_transacciones) FROM RESUMEN_APORTE_SBIF) AS total_monto_resumen
FROM dual;


