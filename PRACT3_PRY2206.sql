SET SERVEROUTPUT ON;


---Caso 1---
-- Año de acreditación (paramétrico). El proceso trabaja con el año anterior.
VAR b_anno_acreditacion NUMBER;
 -- puedes cambiarlo por 2026, 2025, etc.
EXEC :b_anno_acreditacion := EXTRACT(YEAR FROM SYSDATE); 

DECLARE
  v_anno_base NUMBER := :b_anno_acreditacion - 1;

  -- VARRAY con multas por día (Tabla 1)
  TYPE t_multas IS VARRAY(7) OF NUMBER;
  v_multas t_multas := t_multas(1200,1300,1700,1900,1100,2000,2300);

  -- Registro (record) para manejar el “paquete” de datos del cursor
  TYPE r_pago IS RECORD (
    pac_run        paciente.pac_run%TYPE,
    pac_dv         paciente.dv_run%TYPE,
    pnombre        paciente.pnombre%TYPE,
    snombre        paciente.snombre%TYPE,
    apaterno       paciente.apaterno%TYPE,
    amaterno       paciente.amaterno%TYPE,
    fecha_nac      paciente.fecha_nacimiento%TYPE,
    ate_id         atencion.ate_id%TYPE,
    fecha_venc     pago_atencion.fecha_venc_pago%TYPE,
    fecha_pago     pago_atencion.fecha_pago%TYPE,
    esp_nombre     especialidad.nombre%TYPE
  );
  v_reg r_pago;

  -- Cursor explícito: pagos fuera de plazo del año base
  CURSOR c_morosos IS
    SELECT
      p.pac_run,
      p.dv_run,
      p.pnombre,
      p.snombre,
      p.apaterno,
      p.amaterno,
      p.fecha_nacimiento,
      a.ate_id,
      pa.fecha_venc_pago,
      pa.fecha_pago,
      e.nombre AS esp_nombre
    FROM paciente p
    JOIN atencion a      ON a.pac_run = p.pac_run
    JOIN pago_atencion pa ON pa.ate_id = a.ate_id
    JOIN especialidad e  ON e.esp_id = a.esp_id
    WHERE pa.fecha_pago IS NOT NULL
      AND TRUNC(pa.fecha_pago) > TRUNC(pa.fecha_venc_pago)         -- pagado tarde
      AND EXTRACT(YEAR FROM pa.fecha_pago) = v_anno_base            -- año base
    ORDER BY pa.fecha_venc_pago ASC, p.apaterno ASC;

  v_dias_morosidad NUMBER;
  v_multa_dia      NUMBER;
  v_monto_multa    NUMBER;
  v_edad           NUMBER;
  v_desc_pct       NUMBER;

BEGIN
  -- Truncar en tiempo de ejecución (DDL dentro de PL/SQL => EXECUTE IMMEDIATE)
  EXECUTE IMMEDIATE 'TRUNCATE TABLE PAGO_MOROSO';

  OPEN c_morosos;
  LOOP
    FETCH c_morosos INTO v_reg;
    EXIT WHEN c_morosos%NOTFOUND;

    -- Días morosidad
    v_dias_morosidad := TRUNC(v_reg.fecha_pago) - TRUNC(v_reg.fecha_venc);

    -- Multa por día según especialidad (usando condicionales)
    IF v_reg.esp_nombre IN ('Ciruga General','Dermatologa') THEN
      v_multa_dia := v_multas(1);
    ELSIF v_reg.esp_nombre = 'Ortopedia y Traumatologa' THEN
      v_multa_dia := v_multas(2);
    ELSIF v_reg.esp_nombre IN ('Inmunologa','Otorrinolaringologa') THEN
      v_multa_dia := v_multas(3);
    ELSIF v_reg.esp_nombre IN ('Fisiatra','Medicina Interna') THEN
      v_multa_dia := v_multas(4);
    ELSIF v_reg.esp_nombre = 'Medicina General' THEN
      v_multa_dia := v_multas(5);
    ELSIF v_reg.esp_nombre = 'Psiquiatra Adultos' THEN
      v_multa_dia := v_multas(6);
    ELSIF v_reg.esp_nombre IN ('Ciruga Digestiva','Reumatologa') THEN
      v_multa_dia := v_multas(7);
    ELSE
      v_multa_dia := 0; 
    END IF;

    -- Multa total
    v_monto_multa := v_dias_morosidad * v_multa_dia;

    -- Edad (en años) y descuento tercera edad
    v_edad := FLOOR(MONTHS_BETWEEN(SYSDATE, v_reg.fecha_nac) / 12);

    v_desc_pct := 0;
    IF v_edad >= 65 THEN
      BEGIN
        SELECT porcentaje_descto
          INTO v_desc_pct
          FROM porc_descto_3ra_edad
         WHERE v_edad BETWEEN anno_ini AND anno_ter;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          v_desc_pct := 0;
      END;

      v_monto_multa := ROUND(v_monto_multa - (v_monto_multa * (v_desc_pct / 100)));
    END IF;

    -- Insert en tabla destino
    INSERT INTO pago_moroso
      (pac_run, pac_dv_run, pac_nombre, ate_id, fecha_venc_pago, fecha_pago,
       dias_morosidad, especialidad_atencion, monto_multa)
    VALUES
      (v_reg.pac_run,
       v_reg.pac_dv,
       v_reg.apaterno || ' ' || v_reg.amaterno || ', ' || v_reg.pnombre || ' ' || v_reg.snombre,
       v_reg.ate_id,
       v_reg.fecha_venc,
       v_reg.fecha_pago,
       v_dias_morosidad,
       v_reg.esp_nombre,
       v_monto_multa);

  END LOOP;
  CLOSE c_morosos;

  DBMS_OUTPUT.PUT_LINE('Caso 1 OK. Año base procesado: ' || v_anno_base);

END;
/
-- Consulta de control 
SELECT * FROM pago_moroso
ORDER BY fecha_venc_pago, pac_nombre;


---Caso 2---
-- NOTA DEL CASO 2: antes de cada prueba, eliminar y recrear la tabla
DROP TABLE medico_servicio_comunidad PURGE;

CREATE TABLE MEDICO_SERVICIO_COMUNIDAD
(id_med_scomun NUMBER(2) GENERATED ALWAYS AS IDENTITY MINVALUE 1 
MAXVALUE 9999999999999999999999999999
INCREMENT BY 1 START WITH 1
CONSTRAINT PK_MED_SERV_COMUNIDAD PRIMARY KEY,
 unidad VARCHAR2(50) NOT NULL,
 run_medico VARCHAR2(15) NOT NULL,
 nombre_medico VARCHAR2(50) NOT NULL,
 correo_institucional VARCHAR2(25) NOT NULL,
 total_aten_medicas NUMBER(2) NOT NULL,
 destinacion VARCHAR2(50) NOT NULL);

SET SERVEROUTPUT ON;

DECLARE
  v_anno_base NUMBER := EXTRACT(YEAR FROM SYSDATE) - 1;

  -- VARRAY destinaciones (Tabla 2)
  TYPE t_dest IS VARRAY(3) OF VARCHAR2(60);
  v_dest t_dest := t_dest(
    'Servicio de Atención Primaria de Urgencia (SAPU)',
    'Hospitales del área de la Salud Pública',
    'Centros de Salud Familiar (CESFAM)'
  );

  TYPE r_med IS RECORD (
    uni_nombre   unidad.nombre%TYPE,
    med_run      medico.med_run%TYPE,
    med_dv       medico.dv_run%TYPE,
    pnombre      medico.pnombre%TYPE,
    snombre      medico.snombre%TYPE,
    apaterno     medico.apaterno%TYPE,
    amaterno     medico.amaterno%TYPE
  );
  v_reg r_med;

  v_total_atenciones NUMBER := 0;
  v_max_atenciones   NUMBER := 0;
  v_destinacion      VARCHAR2(60);
  v_correo           VARCHAR2(25);

  CURSOR c_medicos IS
    SELECT u.nombre AS uni_nombre,
           m.med_run,
           m.dv_run,
           m.pnombre,
           m.snombre,
           m.apaterno,
           m.amaterno
      FROM medico m
      JOIN unidad u ON u.uni_id = m.uni_id
     ORDER BY u.nombre ASC, m.apaterno ASC;

BEGIN
  -- Máximo de atenciones del año base (si no hay filas, queda 0)
  SELECT NVL(MAX(cnt),0)
    INTO v_max_atenciones
    FROM (
      SELECT med_run, COUNT(*) cnt
        FROM atencion
       WHERE EXTRACT(YEAR FROM fecha_atencion) = v_anno_base
       GROUP BY med_run
    );

  OPEN c_medicos;
  LOOP
    FETCH c_medicos INTO v_reg;
    EXIT WHEN c_medicos%NOTFOUND;

    -- Total atenciones del médico en el año base
    SELECT COUNT(*)
      INTO v_total_atenciones
      FROM atencion
     WHERE med_run = v_reg.med_run
       AND EXTRACT(YEAR FROM fecha_atencion) = v_anno_base;

    -- Solo los que hicieron MENOS que el máximo
    IF v_total_atenciones < v_max_atenciones THEN

      -- Destinación según Tabla 2 (condicionales)
    
      IF v_reg.uni_nombre IN ('ATENCIN ADULTO','ATENCIN AMBULATORIA') THEN
        v_destinacion := v_dest(1); 

      ELSIF v_reg.uni_nombre = 'ATENCIN URGENCIA' THEN
        IF v_total_atenciones BETWEEN 0 AND 3 THEN
          v_destinacion := v_dest(1); 
        ELSE
          v_destinacion := v_dest(2); 
        END IF;

      ELSIF v_reg.uni_nombre IN ('CARDIOLOGA','ONCOLGICA') THEN
        v_destinacion := v_dest(2); 

      ELSIF v_reg.uni_nombre IN ('CIRUGA','CIRUGA PLSTICA') THEN
        IF v_total_atenciones BETWEEN 0 AND 3 THEN
          v_destinacion := v_dest(1); 
        ELSE
          v_destinacion := v_dest(2); 
        END IF;

      ELSIF v_reg.uni_nombre = 'PACIENTE CRTICO' THEN
        v_destinacion := v_dest(2); 

      ELSIF v_reg.uni_nombre = 'PSIQUIATRA Y SALUD MENTAL' THEN
        v_destinacion := v_dest(3); 

      ELSIF v_reg.uni_nombre = 'TRAUMATOLOGA ADULTO' THEN
        IF v_total_atenciones BETWEEN 0 AND 3 THEN
          v_destinacion := v_dest(1); 
        ELSE
          v_destinacion := v_dest(2); 
        END IF;

      ELSE
        v_destinacion := v_dest(2); 
      END IF;

      -- Correo institucional (regla del enunciado)
      -- 2 primeras letras unidad (sin espacios) + (penúltima + antepenúltima del apellido) + últimos 3 dígitos RUN + dominio
      v_correo :=
        SUBSTR(REPLACE(UPPER(v_reg.uni_nombre),' ',''),1,2) ||
        SUBSTR(UPPER(v_reg.apaterno), LENGTH(v_reg.apaterno)-1, 1) ||
        SUBSTR(UPPER(v_reg.apaterno), LENGTH(v_reg.apaterno)-2, 1) ||
        LPAD(MOD(v_reg.med_run,1000),3,'0') ||
        '@ketekura.cl';

      INSERT INTO medico_servicio_comunidad
        (unidad, run_medico, nombre_medico, correo_institucional, total_aten_medicas, destinacion)
      VALUES
        (v_reg.uni_nombre,
         v_reg.med_run || '-' || v_reg.med_dv,
         v_reg.pnombre || ' ' || v_reg.snombre || ' ' || v_reg.apaterno || ' ' || v_reg.amaterno,
         v_correo,
         v_total_atenciones,
         v_destinacion);

    END IF;

  END LOOP;
  CLOSE c_medicos;

  DBMS_OUTPUT.PUT_LINE('Caso 2 OK. Año base: ' || v_anno_base || '. Máximo atenciones: ' || v_max_atenciones);

END;
/
-- Consulta de control---
SELECT unidad, run_medico, nombre_medico, correo_institucional, total_aten_medicas, destinacion
FROM medico_servicio_comunidad
ORDER BY unidad, nombre_medico;

