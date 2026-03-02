/* ============================================================
   PRY2206 - Programación de Bases de Datos
   Exp 3 - Semana 8 (Actividad Sumativa)
   Estudiante: Santiago Vivanco
   ============================================================

   CONTENIDO:
   1) CASO 1: Trigger TRG_TOTAL_CONSUMOS
      - Mantiene TOTAL_CONSUMOS actualizado al insertar, actualizar o eliminar consumos.

   2) CASO 2: Cobranza automática
      - Package: PKG_COBRANZA_HOTEL (monto de tours en USD)
      - Función: FN_OBT_AGENCIA (retorna agencia y registra errores en REG_ERRORES)
      - Función: FN_OBT_CONSUMOS_USD (retorna consumos desde TOTAL_CONSUMOS)
      - Procedimiento: SP_PROCESA_PAGOS (procesa salidas del día, calcula pagos y guarda en DETALLE_DIARIO_HUESPEDES)

*/

/* ============================================================
   CASO 1
   Trigger en CONSUMO que actualiza TOTAL_CONSUMOS
   Reacciona a:
   - INSERT: suma monto al total del huésped
   - UPDATE: ajusta diferencia (nuevo - antiguo)
   - DELETE: resta monto del total del huésped
   ============================================================ */

CREATE OR REPLACE TRIGGER trg_total_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    UPDATE total_consumos
       SET monto_consumos = monto_consumos + :NEW.monto
     WHERE id_huesped = :NEW.id_huesped;

  ELSIF DELETING THEN
    UPDATE total_consumos
       SET monto_consumos = monto_consumos - :OLD.monto
     WHERE id_huesped = :OLD.id_huesped;

  ELSIF UPDATING THEN
    UPDATE total_consumos
       SET monto_consumos = monto_consumos + (:NEW.monto - :OLD.monto)
     WHERE id_huesped = :NEW.id_huesped;
  END IF;
END;
/
SHOW ERRORS TRIGGER trg_total_consumos;


/* ============================================================
   CASO 1 - Bloque de prueba solicitado
   - Inserta nuevo consumo (id siguiente al último) para huésped 340006, reserva 1587, monto 150
   - Elimina consumo id 11473
   - Actualiza consumo id 10688 a monto 95
   ============================================================ */

DECLARE
  v_nuevo_id NUMBER;
BEGIN
  SELECT NVL(MAX(id_consumo),0) + 1
    INTO v_nuevo_id
    FROM consumo;

  INSERT INTO consumo (id_consumo, id_reserva, id_huesped, monto)
  VALUES (v_nuevo_id, 1587, 340006, 150);

  DELETE FROM consumo
   WHERE id_consumo = 11473;

  UPDATE consumo
     SET monto = 95
   WHERE id_consumo = 10688;

  COMMIT;
END;
/
-- Validación rápida caso 1
SELECT * FROM total_consumos WHERE id_huesped = 340006;
SELECT * FROM consumo WHERE id_consumo IN (10688, 11473);


/* ============================================================
   CASO 2
   PACKAGE: PKG_COBRANZA_HOTEL
   Función: FN_MONTO_TOURS_USD
   - Retorna total de tours en USD por huésped
   - Si no tiene tours -> devuelve 0
   ============================================================ */

CREATE OR REPLACE PACKAGE PKG_COBRANZA_HOTEL AS
  g_monto_tours NUMBER;  -- variable opcional

  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER;
END PKG_COBRANZA_HOTEL;
/
SHOW ERRORS PACKAGE PKG_COBRANZA_HOTEL;

CREATE OR REPLACE PACKAGE BODY PKG_COBRANZA_HOTEL AS

  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER IS
    v_total NUMBER;
  BEGIN
    SELECT NVL(SUM(ht.num_personas * t.valor_tour), 0)
      INTO v_total
      FROM huesped_tour ht
      JOIN tour t ON t.id_tour = ht.id_tour
     WHERE ht.id_huesped = p_id_huesped;

    g_monto_tours := v_total;
    RETURN v_total;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      g_monto_tours := 0;
      RETURN 0;
    WHEN OTHERS THEN
      g_monto_tours := 0;
      RETURN 0;
  END FN_MONTO_TOURS_USD;

END PKG_COBRANZA_HOTEL;
/
SHOW ERRORS PACKAGE BODY PKG_COBRANZA_HOTEL;


/* ============================================================
   FUNCIÓN: FN_OBT_AGENCIA
   - Retorna la agencia del huésped (AGENCIA.NOM_AGENCIA)
   - Si ocurre error, registra en REG_ERRORES usando SQ_ERROR
   - Retorna “NO REGISTRA AGENCIA” cuando hay error
   - PRAGMA AUTONOMOUS_TRANSACTION permite insertar en REG_ERRORES incluso desde un SELECT
   ============================================================ */

CREATE OR REPLACE FUNCTION FN_OBT_AGENCIA
(p_id_huesped NUMBER)
RETURN VARCHAR2
IS
  v_agencia      VARCHAR2(35);
  v_msg_error    VARCHAR2(4000);

  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  SELECT a.nom_agencia
    INTO v_agencia
    FROM huesped h
    JOIN agencia a ON a.id_agencia = h.id_agencia
   WHERE h.id_huesped = p_id_huesped;

  RETURN v_agencia;

EXCEPTION
  WHEN OTHERS THEN
    v_msg_error := SUBSTR(NVL(SQLERRM, 'ERROR NO ESPECIFICADO'), 1, 300);

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (
      sq_error.NEXTVAL,
      SUBSTR('FN_OBT_AGENCIA. Huesped: ' || p_id_huesped, 1, 200),
      v_msg_error
    );

    COMMIT;
    RETURN 'NO REGISTRA AGENCIA';
END;
/
SHOW ERRORS FUNCTION FN_OBT_AGENCIA;


/* ============================================================
   FUNCIÓN: FN_OBT_CONSUMOS_USD
   - Obtiene consumos en USD desde TOTAL_CONSUMOS
   - Si no existe registro -> devuelve 0
   ============================================================ */

CREATE OR REPLACE FUNCTION FN_OBT_CONSUMOS_USD
(p_id_huesped NUMBER)
RETURN NUMBER
IS
  v_total NUMBER;
BEGIN
  SELECT tc.monto_consumos
    INTO v_total
    FROM total_consumos tc
   WHERE tc.id_huesped = p_id_huesped;

  RETURN NVL(v_total, 0);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    RETURN 0;
END;
/
SHOW ERRORS FUNCTION FN_OBT_CONSUMOS_USD;


/* ============================================================
   PROCEDIMIENTO PRINCIPAL: SP_PROCESA_PAGOS
   - Parámetros:
     p_fecha_proceso: fecha a procesar (ej: 18/08/2021)
     p_valor_dolar  : tipo de cambio (ej: 915)
   - Procesa huéspedes cuya salida = ingreso + estadia = p_fecha_proceso
   - Calcula:
     alojamiento (habitacion + minibar) diario * estadia
     consumos (TOTAL_CONSUMOS)
     tours (package)
     valor por persona: $35.000 CLP por persona (personas = cantidad de habitaciones de la reserva)
     descuento consumos: tabla TRAMOS_CONSUMOS
     descuento agencia: 12% solo si agencia = “VIAJES ALBERTI”
   - Guarda en DETALLE_DIARIO_HUESPEDES todo en CLP (redondeado)
   - Limpia DETALLE_DIARIO_HUESPEDES y REG_ERRORES antes de procesar
   ============================================================ */

CREATE OR REPLACE PROCEDURE SP_PROCESA_PAGOS
(
  p_fecha_proceso IN DATE,
  p_valor_dolar   IN NUMBER
)
IS
  CURSOR c_huespedes IS
    SELECT r.id_reserva,
           r.id_huesped,
           r.estadia,
           h.appat_huesped,
           h.apmat_huesped,
           h.nom_huesped
      FROM reserva r
      JOIN huesped h ON h.id_huesped = r.id_huesped
     WHERE TRUNC(r.ingreso + r.estadia) = TRUNC(p_fecha_proceso);

  v_agencia             VARCHAR2(40);

  v_aloj_usd            NUMBER := 0;
  v_consumos_usd        NUMBER := 0;
  v_tours_usd           NUMBER := 0;

  v_personas            NUMBER := 0;
  v_valor_personas_usd  NUMBER := 0;

  v_subtotal_usd        NUMBER := 0;
  v_pct_consumos        NUMBER := 0;
  v_desc_consumos_usd   NUMBER := 0;

  v_desc_agencia_usd    NUMBER := 0;
  v_total_usd           NUMBER := 0;

  v_aloj_clp            NUMBER := 0;
  v_consumos_clp        NUMBER := 0;
  v_tours_clp           NUMBER := 0;
  v_subtotal_clp        NUMBER := 0;
  v_desc_consumos_clp   NUMBER := 0;
  v_desc_agencia_clp    NUMBER := 0;
  v_total_clp           NUMBER := 0;

  v_nombre              VARCHAR2(60);

BEGIN
  DELETE FROM detalle_diario_huespedes;
  DELETE FROM reg_errores;
  COMMIT;

  FOR x IN c_huespedes LOOP

    v_nombre := x.appat_huesped || ' ' || x.apmat_huesped || ' ' || x.nom_huesped;

    v_agencia := FN_OBT_AGENCIA(x.id_huesped);

    v_consumos_usd := ROUND(FN_OBT_CONSUMOS_USD(x.id_huesped));

    v_tours_usd := ROUND(PKG_COBRANZA_HOTEL.FN_MONTO_TOURS_USD(x.id_huesped));

    SELECT COUNT(*)
      INTO v_personas
      FROM detalle_reserva dr
     WHERE dr.id_reserva = x.id_reserva;

    v_valor_personas_usd := ROUND((35000 * v_personas) / p_valor_dolar);

    SELECT ROUND(NVL(SUM((hb.valor_habitacion + hb.valor_minibar) * x.estadia), 0))
      INTO v_aloj_usd
      FROM detalle_reserva dr
      JOIN habitacion hb ON hb.id_habitacion = dr.id_habitacion
     WHERE dr.id_reserva = x.id_reserva;

    BEGIN
      SELECT tc.pct
        INTO v_pct_consumos
        FROM tramos_consumos tc
       WHERE v_consumos_usd BETWEEN tc.vmin_tramo AND tc.vmax_tramo;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        v_pct_consumos := 0;
    END;

    v_desc_consumos_usd := ROUND(v_consumos_usd * (v_pct_consumos / 100));

    v_subtotal_usd := ROUND(v_aloj_usd + v_consumos_usd + v_tours_usd + v_valor_personas_usd);

    IF UPPER(TRIM(v_agencia)) = 'VIAJES ALBERTI' THEN
      v_desc_agencia_usd := ROUND(v_subtotal_usd * 0.12);
    ELSE
      v_desc_agencia_usd := 0;
    END IF;

    v_total_usd := ROUND(v_subtotal_usd - v_desc_consumos_usd - v_desc_agencia_usd);

    v_aloj_clp          := ROUND(v_aloj_usd * p_valor_dolar);
    v_consumos_clp      := ROUND(v_consumos_usd * p_valor_dolar);
    v_tours_clp         := ROUND(v_tours_usd * p_valor_dolar);
    v_subtotal_clp      := ROUND(v_subtotal_usd * p_valor_dolar);
    v_desc_consumos_clp := ROUND(v_desc_consumos_usd * p_valor_dolar);
    v_desc_agencia_clp  := ROUND(v_desc_agencia_usd * p_valor_dolar);
    v_total_clp         := ROUND(v_total_usd * p_valor_dolar);

    INSERT INTO detalle_diario_huespedes
    (id_huesped, nombre, agencia, alojamiento, consumos, tours,
     subtotal_pago, descuento_consumos, descuentos_agencia, total)
    VALUES
    (x.id_huesped, v_nombre, SUBSTR(v_agencia,1,40),
     v_aloj_clp, v_consumos_clp, v_tours_clp,
     v_subtotal_clp, v_desc_consumos_clp, v_desc_agencia_clp, v_total_clp);

  END LOOP;

  COMMIT;
END;
/
SHOW ERRORS PROCEDURE SP_PROCESA_PAGOS;


/* ============================================================
   EJECUCIÓN DE PRUEBA (según enunciado)
   “día actual” para pruebas: 18/08/2021
   ============================================================ */

BEGIN
  SP_PROCESA_PAGOS(TO_DATE('18/08/2021','DD/MM/YYYY'), 915);
END;
/
-- Validación final:
SELECT * FROM detalle_diario_huespedes ORDER BY id_huesped;
SELECT * FROM reg_errores ORDER BY id_error;