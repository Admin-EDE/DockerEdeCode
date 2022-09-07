from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3D3(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verificar que los campos ClassMeetingDays y ClassPeriod se encuentren dentro de la lista. 
    Esto ayudará a identificar el segundo bloque de cada curso, lo cual es necesario para 
    comparar el registro de control de asignatura contra el registro de control de subvenciones.
    Tabla OrganizationCalendarSession, campos ClassMeetingDays y ClassPeriod
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existen ClassMeetingDays ni ClassPeriod en CourseSectionSchedule
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los ClassMeetingDays corresponden a los días de la semana, y los ClassPeriod a los bloques
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    _ExistData = []
    try:
        _ExistData = ejecutar_sql(conn, """--sql
        -- Lista todos los registros de la tabla CourseSectionSchedule
        -- si no hay información, se informa SIN DATOS
        SELECT count(ClassMeetingDays), count(ClassPeriod)
        FROM CourseSectionSchedule
      """)
    except Exception as e:
        logger.info(f"Resultado: {_ExistData} -> {str(e)}")

    if(_ExistData[0][0] == 0 and _ExistData[0][1] == 0):
        logger.info(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
    ClassMeetingDays = []
    try:
        ClassMeetingDays = ejecutar_sql(conn, """--sql
        -- Lista todos los registro del campo ClassMeetingDays de la tabla CourseSectionSchedule
        -- que no se encuentren dentro de la lista permitida
        WITH split(word, str) AS (
            SELECT '', ClassMeetingDays||',' FROM CourseSectionSchedule
            UNION ALL SELECT
            substr(str, 0, instr(str, ',')),
            substr(str, instr(str, ',')+1)
            FROM split WHERE str!=''
        ) SELECT DISTINCT word FROM split WHERE word!='' AND word NOT IN ('Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes');
      """)
    except Exception as e:
        logger.info(f"Resultado: {ClassMeetingDays} -> {str(e)}")

    ClassPeriod = []
    try:
        ClassPeriod = ejecutar_sql(conn, """--sql
        -- Lista todos los registro del campo ClassMeetingDays de la tabla CourseSectionSchedule
        -- que no se encuentren dentro de la lista permitida
        WITH split(word, str) AS (
            SELECT '', ClassPeriod||',' FROM CourseSectionSchedule
            UNION ALL SELECT
            substr(str, 0, instr(str, ',')),
            substr(str, instr(str, ',')+1)
            FROM split WHERE str!=''
        ) SELECT DISTINCT word FROM split WHERE word!='' AND word NOT IN ('Bloque01','Bloque02','Bloque03','Bloque04','Bloque05','Bloque06','Bloque07','Bloque08','Bloque09','Bloque10','Bloque11','Bloque12','Bloque13','Bloque14','Bloque15','Bloque16','Bloque17','Bloque18','Bloque19','Bloque20');
      """)
    except Exception as e:
        logger.info(f"Resultado: {ClassPeriod} -> {str(e)}")

    if(len(ClassMeetingDays) == 0 and len(ClassPeriod) == 0):
        logger.info(f"Aprobado")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        if(len(ClassMeetingDays) != 0):
            logger.info(
                f"ClassMeetingDays con formato errorneo: {len(ClassMeetingDays)}")
            data1 = list(
                set([m[0] for m in ClassMeetingDays if m[0] is not None]))
            _c1 = len(set(data1))
            if (_c1 > 0):
                logger.error(
                    f"Las siguientes registros tiene mal formateado el campo ClassMeetingDays: {data1}")

        if(len(ClassPeriod) != 0):
            logger.info(f"ClassPeriod con formato erroneo: {len(ClassPeriod)}")
            data2 = list(set([m[0] for m in ClassPeriod if m[0] is not None]))
            _c2 = len(set(data2))
            if (_c2 > 0):
                logger.error(
                    f"Las siguientes registros tienen mal formateado el campo ClassPeriod: {data2}")

        if (_c1 > 0 or _c2 > 0):
            logger.error(f"Rechazado")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
